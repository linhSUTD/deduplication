import os
import time
import comparison
import dataset
import indexing
import encode

# =============================================================================

arg_data_set_name     =  'rest';

arg_index_method_name =  'blocking';


# =============================================================================

result_file_name    = './results/evalBlocking.res'

# =============================================================================

res_file = open(result_file_name, 'a')
res_file.write(os.linesep+os.linesep+'Experiment started %s:' % (\
               time.strftime('%Y%m%d-%H%M')) + os.linesep)

# =============================================================================

experiment_dict = {}
index_def_dict  = {}
indexing_dict   = {}
result_dict     = {}

# =============================================================================

census_ds = dataset.DataSetCSV(description='Census data set',
                                 access_mode='read',
                                 delimiter='\t',
                                 rec_ident='rec_id',
                                 header_line=False,
                                 field_list=[('relation',0),
                                             ('entity_id',1),
                                             ('surname',2),
                                             ('given_name',3),
                                             ('middle_initial',4),
                                             ('zipcode',5),
                                             ('suburb',6)],
                                 file_name = './data/census.tab')

# Define field and record comparators

census_entity_id_exact      =   comparison.FieldComparatorExactString(desc = 'entity_id_exact')

census_surname_winkler      =   comparison.FieldComparatorJaro(thres=0, desc = 'surname_jaro')

census_given_name_winkler   =   comparison.FieldComparatorJaro(thres=0, desc = 'given_name_jaro')

census_suburb_winkler       =   comparison.FieldComparatorJaro(thres=0, desc = 'suburb_jaro')

census_fc_list = [(census_entity_id_exact,    'entity_id',    'entity_id'),
                  (census_surname_winkler,    'surname',      'surname'),
                  (census_given_name_winkler, 'given_name',   'given_name'),
                  (census_suburb_winkler,     'suburb',       'suburb')]

census_rec_comp = comparison.RecordComparator(census_ds, census_ds,
                                              census_fc_list,
                                              'Census record comparator')

# Function to be used to check for true matches and non-matches
#
def census_check_funct(rec1, rec2):
    return (rec1[1] == rec2[1])

# Function to be used to extract the record identifier from a raw record
#
def census_get_id_funct(rec):
    return rec[1]

# Insert into data set dictionary
#
experiment_dict['census'] = ['Census', census_ds, census_ds, census_rec_comp, census_check_funct, census_get_id_funct]

# Set-up index definitions
#
census_index_def1 = \
    [
        [
            ['surname',     'surname',        False, False, None, [encode.nysiis,3]],
            ['given_name',  'given_name',     False, False, None, [encode.nysiis,3]]
        ],
        [
            ['suburb',      'suburb',         False, False, None, [encode.nysiis,3]],
            ['zipcode',     'zipcode',        False, False, None, []]
        ]
    ]

census_index_def2 = \
    [
        [
            ['surname',         'surname',          False, False,   3,              [encode.nysiis,3]],
            ['middle_initial',  'middle_initial',   False, False,   1,              []],
            ['zipcode',         'zipcode',          False, False, None,             []]
        ],
        [
            ['given_name',      'given_name',       False, False, None,             [encode.nysiis,3]],
            ['suburb',          'suburb',           False, False, None,             [encode.nysiis,3]]
        ]
    ]

census_index_def3 = \
    [
        [
            ['suburb',      'suburb',   False, False, None,             [encode.nysiis,3]],
            ['surname',     'surname',  False, False,    3,             [encode.nysiis,3]]
        ],
        [
            ['zipcode',     'zipcode',  False, False, None,             []],
            ['given_name', 'given_name',False, False, None,             [encode.nysiis,3]]
        ]
    ]

index_def_dict['census'] = [census_index_def1, census_index_def2, census_index_def3]

# =============================================================================

rest_ds = dataset.DataSetCSV(description='Restaurant data set',
                             access_mode='read',
                             rec_ident='rec_id',
                             header_line=True,
                             file_name = './data/restaurant.csv')

# Define field and record comparators
#
rest_class_exact  = comparison.FieldComparatorExactString(desc = 'class_exact')

rest_name_winkler = comparison.FieldComparatorJaro(thres=0, desc = 'name_jaro')

rest_addr_winkler = comparison.FieldComparatorJaro(thres=0, desc = 'addr_jaro')

rest_city_winkler = comparison.FieldComparatorJaro(thres=0, desc = 'city_jaro')

rest_fc_list = [(rest_class_exact,  'class', 'class'),
                (rest_name_winkler, 'name',  'name'),
                (rest_addr_winkler, 'addr',  'addr'),
                (rest_city_winkler, 'city',  'city')]

rest_rec_comp = comparison.RecordComparator(rest_ds, rest_ds, rest_fc_list, 'Restaurant record comparator')

# Function to be used to check for true matches and non-matches

def rest_check_funct(rec1, rec2):
    return (rec1[-1] == rec2[-1])

# Function to be used to extract the record identifier from a raw record

def rest_get_id_funct(rec):
    return rec[-1]

# Insert into data set dictionary

experiment_dict['rest'] = ['Restaurant', rest_ds, rest_ds, rest_rec_comp, rest_check_funct, rest_get_id_funct]

# Set-up index definitions

rest_index_def1 = \
    [
        [
            ['phone', 'phone', False, False, None, [encode.get_substring,0,4]],
            ['type',  'type',  False, False, None, [encode.nysiis,3     ]]
        ],
        [
            ['name', 'name',   False, False, None, [encode.nysiis,3     ]],
            ['city', 'city',   False, False, None, [encode.nysiis,3     ]]
        ]

    ]

rest_index_def2 = \
    [
        [
            ['city', 'city',   False, False, None, [encode.nysiis,3     ]],
            ['phone', 'phone', False, False, None, [encode.get_substring,0,4]]
        ],
        [
            ['addr', 'addr',   False, False, None, [encode.nysiis,3     ]],
            ['city', 'city',   False, False, None, [encode.nysiis,3     ]]
        ]
    ]

rest_index_def3 = \
    [
        [
            ['type', 'type',   False, False, None, [encode.nysiis,3     ]],
            ['name', 'name',   False, False, None, [encode.nysiis,3     ]]
        ],
        [
            ['addr', 'addr',   False, False, None, [encode.nysiis,3     ]],
            ['type',  'type',  False, False, None, [encode.nysiis,3     ]]
        ]
    ]

index_def_dict['rest'] = [rest_index_def1, rest_index_def2, rest_index_def3]

# =============================================================================

data_set_name       = experiment_dict[arg_data_set_name][0]

data_set1           = experiment_dict[arg_data_set_name][1]

data_set2           = experiment_dict[arg_data_set_name][2]

rec_cmp             = experiment_dict[arg_data_set_name][3]

check_match_funct   = experiment_dict[arg_data_set_name][4]

get_id_funct        = experiment_dict[arg_data_set_name][5]

index_def_list      = index_def_dict[arg_data_set_name]

ds_index_list       = []

# Blocking index - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
for this_index_def in index_def_list:

    block_index = indexing.BlockingIndex(description = 'Blocking index',
                                         dataset1 = data_set1,
                                         dataset2 = data_set2,
                                         rec_comparator = rec_cmp,
                                         index_def = this_index_def)

    ds_index_list.append(['blocking', block_index])


# ---------------------------------------------------------------------------
# Run experiments for this data set
#
for (index_name, index_method) in ds_index_list:

    time1 = time.time()

    index_method.build()

    time2 = time.time()

    index_method.compact()

    time3 = time.time()

    num_rec_pairs = index_method.num_rec_pairs

    matches     =  0  # Number of matches

    non_matches = 0  # Number of non-matches

    for (rec_id1, rec_id2_set) in index_method.rec_pair_dict.iteritems():

        rec1 = index_method.rec_cache1[rec_id1]

        for rec_id2 in rec_id2_set:
            rec2 = index_method.rec_cache1[rec_id2]  # From same data set

            if (check_match_funct(rec1, rec2) == True):
                matches += 1
            else:
                non_matches += 1

    assert (matches + non_matches) == num_rec_pairs

    # Get total number of matches possible (without indexing) from data set(s)
    #
    ent_id_dict = {}  # Dictionary with all unique entity identifers and
                        # counts of how often they appear

    for ent_rec in index_method.rec_cache1.itervalues():
        ent_id = get_id_funct(ent_rec)
        ent_id_count = ent_id_dict.get(ent_id, 0) + 1
        ent_id_dict[ent_id] = ent_id_count

    assert sum(ent_id_dict.values()) == len(index_method.rec_cache1)

    total_matches = 0  # Total number of true matches (without indexing)

    for (ent_id, ent_count) in ent_id_dict.iteritems():
        total_matches += ent_count * (ent_count-1) / 2

    # calculate recall: the fraction of the number of true match record pairs produced by the blocking method and the number of
    # true match record pairs in the entire data
    if (total_matches > 0):
        assert total_matches >= matches
        recall = float(matches) / float(total_matches)
    else:
        recall = -1

    # calculate precision: the fraction of the number of true match record pairs and the number of record pairs generated
    # by the blocking method

    precision = float(matches) / float(num_rec_pairs)

    # calculate reduction ratio: the fraction of the number of record pairs produced by the blocking method and the number of
    # record pairs in the entire data
    total_rec_pairs = data_set1.num_records * (data_set1.num_records - 1) / 2

    reduction_ratio = 1 - float(num_rec_pairs) / float(total_rec_pairs)

    # Write into results file - - - - - - - - - - - - - - - - - - - - - - - - -
    # (data set name, index method name, num_rec_pairs, RR, PC, PQ, time and
    # memory usage for for build and compact)
    #

    res_file_str = 'Dataset: %s, Method: %s, Num of Record Pairs: %d, RR: %.4f, Recall: %.4f, Precision: %.4f, Time Processing: %.4f' % \
                   (arg_data_set_name, index_name, num_rec_pairs, reduction_ratio * 100, recall * 100, precision * 100, time3-time1)
    print '    Saved into results file: "%s"' % (res_file_str)

    res_file.write(res_file_str+os.linesep)

    del index_method

print
print

del ds_index_list
del data_set1
del data_set2

print 'Finished experiments at:', time.ctime()

    # =============================================================================
