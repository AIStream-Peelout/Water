"""Tests for extending a frozen basin split with gauges added later."""
import json
import os
import tempfile
import unittest

import pandas as pd

from experiments.catchment_foundation import build_basin_split

BASE = {
    "a1": {"state": "CO", "huc": "140100010101", "group": "140100", "fold": 0},
    "a2": {"state": "CO", "huc": "140100010102", "group": "140100", "fold": 0},
    "b1": {"state": "CO", "huc": "140200010101", "group": "140200", "fold": 1},
    "c1": {"state": "UT", "huc": "140100020101", "group": "140100", "fold": 2},
    "d1": {"state": "UT", "huc": "160200010101", "group": "160200", "fold": 1},
}


def candidate(state: str, huc: str) -> dict:
    """Builds the entry of a gauge that is not in the base split."""
    return {"state": state, "huc": huc, "group": huc[:6]}


class TestExtendSplit(unittest.TestCase):
    """Checks that added gauges never move a site and never train next to a held-out one."""

    def test_sites_of_the_base_split_are_untouched(self):
        new = {"a1": candidate("CO", "999999000000"), "n1": candidate("CO", "140100010103")}
        sites = build_basin_split.extend_split(BASE, new, 3, "v2")
        for site, info in BASE.items():
            self.assertEqual(sites[site], info)
        self.assertEqual(set(sites), set(BASE) | {"n1"})

    def test_a_new_gauge_joins_the_fold_of_its_group_in_its_own_state(self):
        new = {"n1": candidate("CO", "140100010103"), "n2": candidate("UT", "140100020102")}
        sites = build_basin_split.extend_split(BASE, new, 3, "v2")
        self.assertEqual(sites["n1"]["fold"], 0)
        self.assertEqual(sites["n2"]["fold"], 2)
        self.assertEqual(sites["n1"]["since"], "v2")
        self.assertNotIn("since", sites["a1"])

    def test_a_group_known_only_in_another_state_takes_its_lowest_fold(self):
        # WY has no 140100 group; CO holds it in fold 0 and UT in fold 2, so it stays held out.
        new = {"n1": candidate("WY", "140100030101"), "n2": candidate("CO", "160200010102")}
        sites = build_basin_split.extend_split(BASE, new, 3, "v2")
        self.assertEqual(sites["n1"]["fold"], 0)
        self.assertEqual(sites["n2"]["fold"], 1)

    def test_unseen_groups_stay_whole_and_fill_the_lightest_folds(self):
        new = {"o%d" % i: candidate("OR", "170900%06d" % i) for i in range(4)}
        new.update({"p%d" % i: candidate("OR", "171000%06d" % i) for i in range(2)})
        new.update({"q0": candidate("OR", "171100000000"), "x1": candidate("CO", "110200000000")})
        sites = build_basin_split.extend_split(BASE, new, 3, "v2")
        # In a state new to the split the largest group goes to the last fold, not to fold 0.
        self.assertEqual({sites["o%d" % i]["fold"] for i in range(4)}, {2})
        self.assertEqual({sites["p%d" % i]["fold"] for i in range(2)}, {1})
        self.assertEqual(sites["q0"]["fold"], 0)
        # Colorado already holds two sites in fold 0 and one in fold 1: fold 2 is the lightest.
        self.assertEqual(sites["x1"]["fold"], 2)

    def test_manifest_sites_read_state_and_huc_and_keep_a_gauge_once(self):
        with tempfile.TemporaryDirectory() as root:
            for state, rows in (("CO", [("01", "140100010101"), ("02", "140200010101")]),
                                ("UT", [("02", "140200010101"), ("03", "160200010101")])):
                os.makedirs(os.path.join(root, state))
                pd.DataFrame(rows, columns=["site_no", "huc_cd"]).to_csv(
                    os.path.join(root, state, "gauges.csv"), index=False)
            manifest = os.path.join(root, "manifest.json")
            with open(manifest, "w") as handle:
                json.dump({"basins": [{"site_id": "01", "state": "CO"},
                                      {"site_id": "02", "state": "CO"},
                                      {"site_id": "02", "state": "UT"},
                                      {"site_id": "03", "state": "UT"}]}, handle)
            sites = build_basin_split.manifest_sites(manifest, root, 6)
        self.assertEqual(sites["01"], {"state": "CO", "huc": "140100010101", "group": "140100"})
        self.assertEqual(sites["02"]["state"], "CO")
        self.assertEqual(sites["03"]["group"], "160200")

    def test_the_first_split_breaks_ties_towards_fold_zero(self):
        groups = {"big": ["a", "b"], "small": ["c"]}
        self.assertEqual(build_basin_split.assign_folds(groups, 3), {"big": 0, "small": 1})
        self.assertEqual(build_basin_split.assign_folds(groups, 3, ties_high=True),
                         {"big": 2, "small": 1})
        self.assertEqual(build_basin_split.assign_folds(groups, 3, load=[5, 0, 1]),
                         {"big": 1, "small": 2})

    def test_fold_summary_counts_every_state(self):
        summary = build_basin_split.fold_summary(BASE, ["CO", "UT"], 3)
        self.assertEqual(summary[0], {"CO": 2, "UT": 0, "total": 2})
        self.assertEqual(summary[1], {"CO": 1, "UT": 1, "total": 2})


if __name__ == "__main__":
    unittest.main()
