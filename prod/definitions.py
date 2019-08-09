"""
This module contains the mappings/definitions
"""


MAINTENANCE = "Maintenance"
POLE_REPLACEMENTS = "Pole Replacements"
CAPACITY = "Capacity"
RELIABILITY = "Reliability"
WRO = "WRO"
RULE_20 = "Rule 20"
EMERGENCY = "Emergency"
UNAVAILABLE = "Unavailable"

GRC_MWC_CODE = {
    POLE_REPLACEMENTS: "7",
    MAINTENANCE: "2",
    CAPACITY: "6",
    RELIABILITY: "8",
    WRO: "10 & 16",
    RULE_20: "30",
    EMERGENCY: "17 & 95",
    UNAVAILABLE: "Blank"
}

GRC_MWC_MAPPINGS = {
    "07": POLE_REPLACEMENTS, "GA": POLE_REPLACEMENTS, "2A": MAINTENANCE,
    "48": MAINTENANCE, "2B": MAINTENANCE, "3G": MAINTENANCE,
    "06": CAPACITY,
    "30": RULE_20,
    "10": WRO, "16": WRO,
    "08": RELIABILITY, "49": RELIABILITY, "56": RELIABILITY,
    "09": RELIABILITY, "50": RELIABILITY,
    "17": EMERGENCY, "95": EMERGENCY, "59": EMERGENCY,
}


MWC_SELECTED_CODES = ["05", "06", "07", "08", "09", "10", "16", "17",
                      "21", "2A", "2B", "2C", "2F", "30", "3M", "46",
                      "48", "49", "54", "56", "58", "59", "63", "95",
                      "AB", "BA", "BF", "BH", "BK", "DD", "EV", "EW",
                      "FZ", "GA", "GC", "GE", "HG", "HN", "HX", "IF",
                      "IS", "JV", "KA", "KB", "KC", "OM", "OS", "NULL"]

HMWPE_SELECTED_CODES = ["120", "121", "122", "140", "141", "142", "143",
                        "144", "145", "166", "168", "170", "171", "221", "222",
                        "231", "232", "233", "234", "235", "236", "237",
                        "241", "242", "243", "244", "261", "262", "263"]

HMWPE_STATUS_CODES = {
    "5": "INSERVICE",
    "30": "IDLE",
    "0": "PROPOSED INSTALL"
}


HMWPE_CIRCUIT_TYPES= {
    "3": "RADIAL",
    "1": "NULLCIRCUIT",
    "2": "NETWORK"
}