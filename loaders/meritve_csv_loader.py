import re
from typing import Dict, List

import pandas as pd


class LoadMeritveFromCsv:
    def __init__(self, file_path) -> None:
        self.file_path: str = file_path
        self.irns: Dict[int, str] = self._load_headers()
        self.skip_lines = len(self.irns) + 1

    def load(self) -> pd.DataFrame:
        return self._read_mertive_csv()

    def _load_headers(self) -> Dict[int, str]:
        irns = self._read_irn_header()
        irns = self._parse_irn_header(irns)
        return irns

    def _read_irn_header(self) -> List[str]:
        RE_IRNS = re.compile(r"[\w\d]+\sirn\s=\s\d+")
        irns = []
        with open(self.file_path, "r") as input_file:
            for line in input_file:
                if RE_IRNS.search(line):
                    irns.append(line.strip())
                else:
                    break
        return irns

    def _parse_irn_header(self, raw_header: List[str]) -> Dict[int, str]:
        irns = {}
        for irn in raw_header:
            splitted_irn = irn.split()
            irns[int(splitted_irn[-1])] = splitted_irn[0]
        return irns

    def _read_mertive_csv(self) -> pd.DataFrame:
        data = pd.read_csv(self.file_path, skiprows=self.skip_lines, delimiter=";", dtype={"SYSTIME": str})
        data = data.iloc[:, :-1]  # odstranimo zadnji stolpec
        data.drop(columns=["TIMESTAMP"], inplace=True)
        # uredimo systime v datetime format
        data["SYSTIME"] = pd.to_datetime(data["SYSTIME"], format="%Y%m%d%H%M%S%f")
        data["OBE_IRN"] = data["OBE_IRN"].map(self.irns)
        data.dropna(axis=0, how="any", inplace=True)
        return data
