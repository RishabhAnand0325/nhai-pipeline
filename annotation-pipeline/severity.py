def severity_rank(severity):
    ranks = {"high": 3, "medium": 2, "low": 1, None: 0}
    return ranks.get(severity, 0)