# Any update to this dict requires updates to the motif_matched_types database
# table definition

def motif_match_types_dict():
    motif_match_types = {
        'exact': 1,
        'all_in_range': 2,
        'in_range': 3,
        'not_similar_enough': 4,
        'INVALIDATED': 5,
        'distance': 6,
    }
    return motif_match_types
