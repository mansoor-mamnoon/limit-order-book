#pragma once
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include "types.hpp"
#include "price_levels.hpp"

namespace lob {

// Forward declaration to avoid header cycles
struct NewOrder;

// ---------- Binary record layouts (for tests/replay) ----------
#pragma pack(push, 1)
struct TradeBin {
  Timestamp ts;
  Tick      px;
  Quantity  qty;
  uint8_t   liquidity_side; // 0=passive bid filled; 1=passive ask filled
  OrderId   passive_id;
  OrderId   taker_id;
  SeqNo     seq;            // sequence at which trade was logged (event counter)
};

enum class EventType : uint8_t { NewLimit=0, NewMarket=1, Cancel=2 };

struct EventBin {
  Timestamp ts;
  EventType type;
  SeqNo     seq;
  OrderId   id;
  UserId    user;
  uint8_t   side;    // 0=Bid, 1=Ask
  Tick      price;
  Quantity  qty;
  uint8_t   is_limit; // for "New" events
};
#pragma pack(pop)

// ---------- Snapshot format (v2: includes per-order FIFO) ----------
#pragma pack(push, 1)
struct SnapshotHeader {
  uint32_t magic   = 0x4C4F4253; // "LOBS"
  uint32_t version = 2;          // v2 = has orders section
  SeqNo     seq    = 0;
  Timestamp ts     = 0;
  uint32_t n_levels= 0;
  uint32_t n_orders= 0;
};

struct SnapshotLevelRec {
  uint8_t  side;  // 0 bid, 1 ask
  Tick     px;
  Quantity total;
};

struct SnapshotOrderRec {
  uint8_t   side;  // 0 bid, 1 ask
  Tick      px;
  OrderId   id;
  UserId    user;
  Quantity  qty;
  Timestamp ts;
  uint32_t  flags;
};
#pragma pack(pop)

// Write a snapshot of current books (bids, asks) including FIFO orders.
class SnapshotWriter {
public:
  explicit SnapshotWriter(std::string out_dir) : out_dir_(std::move(out_dir)) {}
  static std::string join(const std::string& a, const std::string& b) {
    if (!a.empty() && a.back() == '/') return a + b;
    return a + "/" + b;
  }
  std::string path_for(SeqNo seq) const {
    return join(out_dir_, "snapshot_seq" + std::to_string(seq) + ".bin");
  }

  bool write_snapshot(const IPriceLevels& bids,
                      const IPriceLevels& asks,
                      SeqNo seq, Timestamp ts);

private:
  std::string out_dir_;
};

// Load a snapshot back into ladders (rebuilds per-price FIFO)
bool load_snapshot_file(const std::string& path,
                        IPriceLevels& bids,
                        IPriceLevels& asks,
                        SeqNo& out_seq,
                        Timestamp& out_ts);

// ---------- Event logging interface ----------
class IEventLogger {
public:
  virtual ~IEventLogger() = default;

  virtual void log_new(const NewOrder& o, bool is_limit,
                       Tick px_used, Timestamp eff_ts) = 0;

  virtual void log_fill(Tick px, Quantity qty, Side liquidity_side,
                        OrderId passive_id, OrderId taker_id, Timestamp ts) = 0;

  virtual void log_cancel(OrderId id, Timestamp ts) = 0;

  // Hook: lets BookCore provide ladders without RTTI. Default no-op.
  virtual void set_snapshot_sources(const IPriceLevels* /*bids*/,
                                    const IPriceLevels* /*asks*/) {}

  // NEW: called by BookCore after it finishes mutating state for an event.
  // Implementations can trigger periodic snapshots here.
  virtual void on_book_after_event(Timestamp /*ts*/) {}

  virtual void flush() {}
};

// ---------- Concrete logger (jsonl + binary) ----------
class JsonlBinLogger final : public IEventLogger {
public:
  // base_path (without extension) e.g. "d4_artifacts/runA"
  JsonlBinLogger(const std::string& base_path,
                 int snapshot_every,
                 SnapshotWriter* snap_writer);

  // paths used by tests
  std::string trades_bin_path() const { return trades_path_; }
  std::string events_bin_path() const { return events_path_; }

  // IEventLogger
  void log_new(const NewOrder& o, bool is_limit,
               Tick px_used, Timestamp eff_ts) override;

  void log_fill(Tick px, Quantity qty, Side liquidity_side,
                OrderId passive_id, OrderId taker_id, Timestamp ts) override;

  void log_cancel(OrderId id, Timestamp ts) override;

  void set_snapshot_sources(const IPriceLevels* bids,
                            const IPriceLevels* asks) override;

  void on_book_after_event(Timestamp ts) override; // <-- new

  void flush() override;

private:
  void maybe_snapshot(Timestamp ts);

  // files/streams
  std::string base_path_;
  std::string jsonl_path_;
  std::string trades_path_;
  std::string events_path_;
  std::ofstream jsonl_;
  std::ofstream trades_bin_;
  std::ofstream events_bin_;

  // snapshot
  int snapshot_every_{0};
  SeqNo event_count_{0}; // increments on 'new' and 'cancel'
  SnapshotWriter* snap_{nullptr};
  const IPriceLevels* bids_src_{nullptr};
  const IPriceLevels* asks_src_{nullptr};
};

} // namespace lob
