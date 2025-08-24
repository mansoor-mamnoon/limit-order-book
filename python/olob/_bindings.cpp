// python/lob/_bindings.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

#include "lob/book_core.hpp"
#include "lob/logging.hpp"
#include "lob/price_levels.hpp"

namespace py = pybind11;
using namespace lob;

struct PyBook {
  PriceLevelsSparse bids;
  PriceLevelsSparse asks;
  BookCore          core;

  PyBook() : bids(), asks(), core(bids, asks, /*logger*/ nullptr) {}

  ExecResult submit_limit(const NewOrder& o) { return core.submit_limit(o); }
  ExecResult submit_market(const NewOrder& o) { return core.submit_market(o); }
  bool       cancel(OrderId id) { return core.cancel(id); }
  ExecResult modify(const ModifyOrder& m) { return core.modify(m); }

  // L1 snapshot as dict
  py::dict l1() const {
    py::dict d;
    const Tick bb = bids.best_bid();
    const Tick ba = asks.best_ask();

    auto qty_at = [](const IPriceLevels& side, Tick px) -> Quantity {
      if (px == std::numeric_limits<Tick>::min() ||
          px == std::numeric_limits<Tick>::max()) {
        return 0;
      }
      const LevelFIFO& L = const_cast<IPriceLevels&>(side).get_level(px);
      return L.total_qty;
    };

    d["best_bid_px"]  = (bb == std::numeric_limits<Tick>::min()) ? py::none() : py::cast(bb);
    d["best_bid_qty"] = (bb == std::numeric_limits<Tick>::min()) ? 0 : qty_at(bids, bb);
    d["best_ask_px"]  = (ba == std::numeric_limits<Tick>::max()) ? py::none() : py::cast(ba);
    d["best_ask_qty"] = (ba == std::numeric_limits<Tick>::max()) ? 0 : qty_at(asks, ba);
    return d;
  }

  // L2 snapshot (top N levels per side), each as list of (px, qty) sorted
  py::dict l2(int depth = 5) const {
    std::vector<std::pair<Tick, Quantity>> bid_levels;
    std::vector<std::pair<Tick, Quantity>> ask_levels;

    bids.for_each_nonempty([&](Tick px, const LevelFIFO& L) {
      bid_levels.emplace_back(px, L.total_qty);
    });
    asks.for_each_nonempty([&](Tick px, const LevelFIFO& L) {
      ask_levels.emplace_back(px, L.total_qty);
    });

    std::sort(bid_levels.begin(), bid_levels.end(),
              [](auto& a, auto& b) { return a.first > b.first; });
    std::sort(ask_levels.begin(), ask_levels.end(),
              [](auto& a, auto& b) { return a.first < b.first; });

    if (static_cast<int>(bid_levels.size()) > depth) bid_levels.resize(depth);
    if (static_cast<int>(ask_levels.size()) > depth) ask_levels.resize(depth);

    py::dict d;
    d["bids"] = bid_levels;
    d["asks"] = ask_levels;
    return d;
  }
};

// Tiny replay helper: expose snapshot load for scripting
py::tuple load_snapshot(const std::string& path) {
  PriceLevelsSparse bids, asks;
  SeqNo seq = 0;
  Timestamp ts = 0;
  bool ok = load_snapshot_file(path, bids, asks, seq, ts);
  return py::make_tuple(ok, seq, ts);
}

PYBIND11_MODULE(_lob, m) {
  // --- enums / constants ---
  py::enum_<Side>(m, "Side")
      .value("Bid", Side::Bid)
      .value("Ask", Side::Ask);

  // Expose bitmask flags as Python ints (cast to underlying type explicitly)
  m.attr("IOC")       = py::int_(static_cast<uint32_t>(OrderFlags::IOC));
  m.attr("FOK")       = py::int_(static_cast<uint32_t>(OrderFlags::FOK));
  m.attr("POST_ONLY") = py::int_(static_cast<uint32_t>(OrderFlags::POST_ONLY));
  m.attr("STP")       = py::int_(static_cast<uint32_t>(OrderFlags::STP));

  // --- data classes / PODs ---
  py::class_<NewOrder>(m, "NewOrder")
      .def(py::init<>())
      .def_readwrite("seq", &NewOrder::seq)
      .def_readwrite("ts", &NewOrder::ts)
      .def_readwrite("id", &NewOrder::id)
      .def_readwrite("user", &NewOrder::user)
      .def_readwrite("side", &NewOrder::side)
      .def_readwrite("price", &NewOrder::price)
      .def_readwrite("qty", &NewOrder::qty)
      .def_readwrite("flags", &NewOrder::flags);

  py::class_<ModifyOrder>(m, "ModifyOrder")
      .def(py::init<>())
      .def_readwrite("seq", &ModifyOrder::seq)
      .def_readwrite("ts", &ModifyOrder::ts)
      .def_readwrite("id", &ModifyOrder::id)
      .def_readwrite("new_price", &ModifyOrder::new_price)
      .def_readwrite("new_qty", &ModifyOrder::new_qty)
      .def_readwrite("flags", &ModifyOrder::flags);

  py::class_<ExecResult>(m, "ExecResult")
      .def_readonly("filled", &ExecResult::filled)
      .def_readonly("remaining", &ExecResult::remaining);

  // --- Book wrapper ---
  py::class_<PyBook>(m, "Book")
      .def(py::init<>())
      .def("submit_limit", &PyBook::submit_limit)
      .def("submit_market", &PyBook::submit_market)
      .def("cancel", &PyBook::cancel)
      .def("modify", &PyBook::modify)
      .def("l1", &PyBook::l1)
      .def("l2", &PyBook::l2, py::arg("depth") = 5);

  // --- helpers ---
  m.def("load_snapshot", &load_snapshot, "Load snapshot and return (ok, seq, ts)");
}
