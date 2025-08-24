#include "lob/logging.hpp"
#include "lob/book_core.hpp" // for NewOrder definition
#include <limits>
#include <filesystem>

namespace lob {

// ---- SnapshotWriter ----
bool SnapshotWriter::write_snapshot(const IPriceLevels& bids,
                                    const IPriceLevels& asks,
                                    SeqNo seq, Timestamp ts) {
  std::filesystem::create_directories(out_dir_);
  std::string path = path_for(seq);
  std::ofstream out(path, std::ios::binary);
  if (!out) return false;

  // Collect non-empty levels (px,total_qty) for both sides
  std::vector<SnapshotLevelRec> lvls;
  bids.for_each_nonempty([&](Tick px, const LevelFIFO& L){
    lvls.push_back(SnapshotLevelRec{0, px, L.total_qty});
  });
  asks.for_each_nonempty([&](Tick px, const LevelFIFO& L){
    lvls.push_back(SnapshotLevelRec{1, px, L.total_qty});
  });

  // Collect per-order FIFO
  std::vector<SnapshotOrderRec> orders;
  bids.for_each_order([&](Tick px, OrderNode* n){
    orders.push_back(SnapshotOrderRec{0, px, n->id, n->user, n->qty, n->ts, n->flags});
  });
  asks.for_each_order([&](Tick px, OrderNode* n){
    orders.push_back(SnapshotOrderRec{1, px, n->id, n->user, n->qty, n->ts, n->flags});
  });

  SnapshotHeader hdr;
  hdr.seq = seq;
  hdr.ts  = ts;
  hdr.n_levels = static_cast<uint32_t>(lvls.size());
  hdr.n_orders = static_cast<uint32_t>(orders.size());

  out.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
  if (!out) return false;

  if (!lvls.empty()) {
    out.write(reinterpret_cast<const char*>(lvls.data()), sizeof(SnapshotLevelRec) * lvls.size());
    if (!out) return false;
  }

  if (!orders.empty()) {
    out.write(reinterpret_cast<const char*>(orders.data()), sizeof(SnapshotOrderRec) * orders.size());
    if (!out) return false;
  }

  return true;
}

// ---- load_snapshot_file ----
bool load_snapshot_file(const std::string& path,
                        IPriceLevels& bids,
                        IPriceLevels& asks,
                        SeqNo& out_seq,
                        Timestamp& out_ts) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;

  SnapshotHeader hdr{};
  in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
  if (!in) return false;
  if (hdr.magic != 0x4C4F4253) return false;

  out_seq = hdr.seq;
  out_ts  = hdr.ts;

  // reset bests to empty
  bids.set_best_bid(std::numeric_limits<Tick>::min());
  asks.set_best_ask(std::numeric_limits<Tick>::max());

  // Back-compat v1: only levels existed.
  if (hdr.version == 1) {
    Tick best_bid = std::numeric_limits<Tick>::min();
    Tick best_ask = std::numeric_limits<Tick>::max();
    for (uint32_t i = 0; i < hdr.n_levels; ++i) {
      SnapshotLevelRec rec{};
      in.read(reinterpret_cast<char*>(&rec), sizeof(rec));
      if (!in) return false;
      if (rec.side == 0) {
        auto& L = bids.get_level(rec.px);
        L.total_qty = rec.total;
        if (rec.total > 0 && rec.px > best_bid) best_bid = rec.px;
      } else {
        auto& L = asks.get_level(rec.px);
        L.total_qty = rec.total;
        if (rec.total > 0 && rec.px < best_ask) best_ask = rec.px;
      }
    }
    if (best_bid != std::numeric_limits<Tick>::min()) bids.set_best_bid(best_bid);
    if (best_ask != std::numeric_limits<Tick>::max()) asks.set_best_ask(best_ask);
    return true;
  }

  // v2: read levels, then per-order FIFO records
  std::vector<SnapshotLevelRec> lvls(hdr.n_levels);
  for (uint32_t i = 0; i < hdr.n_levels; ++i) {
    in.read(reinterpret_cast<char*>(&lvls[i]), sizeof(SnapshotLevelRec));
    if (!in) return false;
  }

  Tick best_bid = std::numeric_limits<Tick>::min();
  Tick best_ask = std::numeric_limits<Tick>::max();

  for (uint32_t i = 0; i < hdr.n_orders; ++i) {
    SnapshotOrderRec rec{};
    in.read(reinterpret_cast<char*>(&rec), sizeof(rec));
    if (!in) return false;

    if (rec.side == 0) {
      LevelFIFO& L = bids.get_level(rec.px);
      auto* n = new OrderNode{rec.id, rec.user, rec.qty, rec.ts, rec.flags, nullptr, nullptr};
      // enqueue
      n->next = nullptr;
      n->prev = L.tail;
      if (L.tail) L.tail->next = n; else L.head = n;
      L.tail = n;
      L.total_qty += n->qty;
      if (rec.px > best_bid) best_bid = rec.px;
    } else {
      LevelFIFO& L = asks.get_level(rec.px);
      auto* n = new OrderNode{rec.id, rec.user, rec.qty, rec.ts, rec.flags, nullptr, nullptr};
      n->next = nullptr;
      n->prev = L.tail;
      if (L.tail) L.tail->next = n; else L.head = n;
      L.tail = n;
      L.total_qty += n->qty;
      if (rec.px < best_ask) best_ask = rec.px;
    }
  }

  if (best_bid != std::numeric_limits<Tick>::min()) bids.set_best_bid(best_bid);
  if (best_ask != std::numeric_limits<Tick>::max()) asks.set_best_ask(best_ask);

  return true;
}

// ---- JsonlBinLogger ----
JsonlBinLogger::JsonlBinLogger(const std::string& base_path,
                               int snapshot_every,
                               SnapshotWriter* snap_writer)
  : base_path_(base_path),
    jsonl_path_(base_path + ".jsonl"),
    trades_path_(base_path + ".trades.bin"),
    events_path_(base_path + ".events.bin"),
    snapshot_every_(snapshot_every),
    snap_(snap_writer) {
  jsonl_.open(jsonl_path_, std::ios::out | std::ios::trunc);
  trades_bin_.open(trades_path_, std::ios::binary | std::ios::trunc);
  events_bin_.open(events_path_, std::ios::binary | std::ios::trunc);
}

void JsonlBinLogger::set_snapshot_sources(const IPriceLevels* bids,
                                          const IPriceLevels* asks) {
  bids_src_ = bids;
  asks_src_ = asks;
}

void JsonlBinLogger::log_new(const NewOrder& o, bool is_limit,
                             Tick px_used, Timestamp eff_ts) {
  EventBin e{};
  e.ts      = eff_ts;
  e.type    = is_limit ? EventType::NewLimit : EventType::NewMarket;
  e.seq     = o.seq;
  e.id      = o.id;
  e.user    = o.user;
  e.side    = (o.side == Side::Bid ? 0u : 1u);
  e.price   = px_used;
  e.qty     = o.qty;
  e.is_limit= is_limit ? 1 : 0;

  events_bin_.write(reinterpret_cast<const char*>(&e), sizeof(e));

  if (jsonl_) {
    jsonl_ << "{\"type\":\"new\",\"is_limit\":" << (is_limit ? "true":"false")
           << ",\"seq\":" << o.seq << ",\"ts\":" << eff_ts
           << ",\"side\":\"" << (o.side==Side::Bid ? "B":"A") << "\""
           << ",\"px\":" << px_used
           << ",\"qty\":" << o.qty << ",\"id\":" << o.id
           << ",\"user\":" << o.user << "}\n";
  }

  ++event_count_;
  // NOTE: snapshot is now triggered by on_book_after_event(), AFTER book mutation.
}

void JsonlBinLogger::log_fill(Tick px, Quantity qty, Side liquidity_side,
                              OrderId passive_id, OrderId taker_id, Timestamp ts) {
  TradeBin t{};
  t.ts = ts;
  t.px = px;
  t.qty = qty;
  t.liquidity_side = (liquidity_side == Side::Bid) ? 0u : 1u;
  t.passive_id = passive_id;
  t.taker_id   = taker_id;
  t.seq        = event_count_;
  trades_bin_.write(reinterpret_cast<const char*>(&t), sizeof(t));

  if (jsonl_) {
    jsonl_ << "{\"type\":\"fill\",\"ts\":" << ts
           << ",\"px\":" << px << ",\"qty\":" << qty
           << ",\"liq_side\":\"" << (liquidity_side==Side::Bid?"B":"A") << "\""
           << ",\"passive_id\":" << passive_id
           << ",\"taker_id\":"   << taker_id
           << ",\"seq\":" << t.seq << "}\n";
  }
}

void JsonlBinLogger::log_cancel(OrderId id, Timestamp ts) {
  EventBin e{};
  e.ts    = ts;
  e.type  = EventType::Cancel;
  e.seq   = event_count_ + 1; // count this cancel as an event
  e.id    = id;
  e.user  = 0;
  e.side  = 0;
  e.price = 0;
  e.qty   = 0;
  e.is_limit = 0;

  events_bin_.write(reinterpret_cast<const char*>(&e), sizeof(e));

  if (jsonl_) {
    jsonl_ << "{\"type\":\"cancel\",\"ts\":" << ts
           << ",\"id\":" << id << ",\"seq\":" << e.seq << "}\n";
  }

  ++event_count_;
  // NOTE: snapshot is now triggered by on_book_after_event(), AFTER book mutation.
}

void JsonlBinLogger::on_book_after_event(Timestamp ts) {
  maybe_snapshot(ts);
}

void JsonlBinLogger::maybe_snapshot(Timestamp ts) {
  if (!snap_ || snapshot_every_ <= 0) return;
  if (event_count_ % static_cast<SeqNo>(snapshot_every_) != 0) return;
  if (!bids_src_ || !asks_src_) return;
  snap_->write_snapshot(*bids_src_, *asks_src_, event_count_, ts);
}

void JsonlBinLogger::flush() {
  if (jsonl_) jsonl_.flush();
  if (trades_bin_) trades_bin_.flush();
  if (events_bin_) events_bin_.flush();
}

} // namespace lob
