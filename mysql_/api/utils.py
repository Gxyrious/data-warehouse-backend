
import difflib

def similar(s1, s2):
    seq = difflib.SequenceMatcher(lambda s: s in (" ", '"'), s1, s2)
    return bool(seq.ratio() > 0.9)