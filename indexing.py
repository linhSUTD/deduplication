
"""Module with classes for indexing/blocking.

    This module provides classes for building blocks and indices that will be
    used in the linkage process for comparing record pairs in an efficient way.

    Various derived classes are provided that implement different indexing
    techniques:

    BlockingIndex            The 'standard' blocking index used for record
                             linkage.

    SortingIndex             Based on a sliding window over the sorted values
                             of the index variable definitions - uses an
                             inverted index approach where keys are unique
                             index variable values.

    SortingArrayIndex        Based on a sliding window over the sorted values
                             of the index variable definitions - uses an array
                             based approach where all index variable values
                             (including duplicates) are stored.
"""

# =============================================================================
# Import necessary modules (Python standard modules first, then Febrl modules)
import gc
import logging

# =============================================================================

class Indexing:

    def __init__(self, base_kwargs):

        self.description     =   ''

        self.dataset1        =   None

        self.dataset2        =   None

        self.rec_comparator  =   None

        self.index_def       =   None

        self.index_sep_str   =   ''

        self.skip_missing    =   True

        self.index1     = {}

        self.index2     = {}

        self.rec_cache1 = {}

        self.rec_cache2 = {}

        self.num_rec_pairs    = None

        self.do_deduplication = True

        self.comp_field_used1 = []

        self.comp_field_used2 = []

        self.rec_length_cache = {}

        for (keyword, value) in base_kwargs.items():

            if (keyword.startswith('desc')):
                self.description = value

            elif (keyword == 'dataset1'):
                self.dataset1 = value

            elif (keyword == 'dataset2'):
                self.dataset2 = value

            elif (keyword.startswith('index_d')):
                self.index_def = value

            elif (keyword.startswith('index_sep')):
                self.index_sep_str = value

            elif (keyword.startswith('rec_com')):
                self.rec_comparator = value

            elif (keyword.startswith('skip')):
                self.skip_missing = value

            else:
                logging.exception('Illegal constructor argument keyword: '+keyword)
                raise Exception

        dataset1_field_names = []

        dataset2_field_names = []

        for (field_name, field_data) in self.dataset1.field_list:
            dataset1_field_names.append(field_name)

        for (field_name, field_data) in self.dataset2.field_list:
            dataset2_field_names.append(field_name)

        for (comp,f_ind1, f_ind2) in self.rec_comparator.field_comparison_list:
            if (f_ind1 not in self.comp_field_used1):
                self.comp_field_used1.append(f_ind1)

            if (f_ind2 not in self.comp_field_used2):
                self.comp_field_used2.append(f_ind2)

        self.comp_field_used1.sort()

        self.comp_field_used2.sort()

        self.index_def_proc = []

        for index_def_list in self.index_def:

            index_def_list_proc = []

            for index_def in index_def_list:

                if (index_def == []):
                    logging.info('Empty index definition given: %s' % (str(self.index_def)))
                    raise Exception

                field_name1 = index_def[0]

                field_name2 = index_def[1]

                field_index1 = dataset1_field_names.index(field_name1)

                field_index2 = dataset2_field_names.index(field_name2)

                index_def_proc = [field_index1,field_index2]

                index_def_proc.append(index_def[2])

                index_def_proc.append(index_def[3])

                if (index_def[4] == None):
                    index_def_proc.append(None)
                else:
                    index_def_proc.append(index_def[4])

                if ((index_def[5] != None) and (len(index_def[5]) > 0)):
                    index_funct_def = index_def[5]
                    index_def_proc.append(index_funct_def)
                else:
                    index_def_proc.append(None)

                index_def_list_proc.append(index_def_proc)

            self.index_def_proc.append(index_def_list_proc)

        assert len(self.index_def) == len(self.index_def_proc)

        self.status = 'initialised'

    # ---------------------------------------------------------------------------

    def __records_into_inv_index__(self):

        num_indices = len(self.index_def)

        for i in range(num_indices):
            self.index1[i] = {}
            self.index2[i] = {}

        skip_missing    = self.skip_missing

        get_index_values_funct = self.__get_index_values__

        build_list = [(self.index1, self.rec_cache1, self.dataset1, self.comp_field_used1, 0)]

        for (index, rec_cache, dataset, comp_field_used_list, ds_index) in build_list:

            for (rec_ident, rec) in dataset.readall():

                comp_rec = []

                field_ind = 0

                for field in rec:
                    if (field_ind in comp_field_used_list):
                        comp_rec.append(field.lower())
                    else:
                        comp_rec.append('')
                    field_ind += 1

                rec_cache[rec_ident] = comp_rec

                rec_index_val_list = get_index_values_funct(rec, ds_index)

                for i in range(num_indices):

                    this_index  = index[i]

                    block_val   = rec_index_val_list[i]

                    if ((block_val != '') or (skip_missing == False)):
                        block_val_rec_list = this_index.get(block_val, [])
                        block_val_rec_list.append(rec_ident)
                        this_index[block_val] = block_val_rec_list

    # ---------------------------------------------------------------------------

    def __dedup_rec_pairs__(self, rec_id_list, rec_pair_dict):

        rec_cnt = 1

        this_rec_id_list = rec_id_list[:]

        this_rec_id_list.sort()

        for rec_ident1 in this_rec_id_list:

            rec_ident2_set = rec_pair_dict.get(rec_ident1, set())

            for rec_ident2 in this_rec_id_list[rec_cnt:]:

                assert rec_ident1 != rec_ident2

                rec_ident2_set.add(rec_ident2)

            rec_pair_dict[rec_ident1] = rec_ident2_set

            rec_cnt += 1

        del this_rec_id_list

    # ---------------------------------------------------------------------------
    def __get_field_names_list__(self):

        field_names_list = []

        for field_comp_tuple in self.rec_comparator.field_comparator_list:
            field_names_list.append(field_comp_tuple[0].description)
        return field_names_list

    # ---------------------------------------------------------------------------

    def __compare_rec_pairs_from_dict__(self, length_filter_perc = None, cut_off_threshold = None):

        weight_vec_dict  = {}

        rec_cache1       = self.rec_cache1

        rec_pair_dict    = self.rec_pair_dict

        rec_comp         = self.rec_comparator.compare

        rec_length_cache = self.rec_length_cache

        if (length_filter_perc != None):
            length_filter_perc /= 100.0  # Normalise

        num_rec_pairs_filtered    = 0

        num_rec_pairs_below_thres = 0

        rec_cache2 = self.rec_cache1

        for rec_ident1 in rec_pair_dict:

            rec1 = rec_cache1[rec_ident1]

            if (length_filter_perc != None):
                rec1_len = len(''.join(rec1))

            for rec_ident2 in rec_pair_dict[rec_ident1]:

                rec2 = rec_cache2[rec_ident2]

                do_comp = True

                if (length_filter_perc != None):
                    if (rec_ident2 in rec_length_cache):
                        rec2_len = rec_length_cache[rec_ident2]
                    else:
                        rec2_len = len(''.join(rec2))
                        rec_length_cache[rec_ident2] = rec2_len

                    perc_diff = float(abs(rec1_len - rec2_len)) / max(rec1_len, rec2_len)

                    if (perc_diff > length_filter_perc):
                        do_comp = False
                        num_rec_pairs_filtered += 1

                if (do_comp == True):
                    w_vec = rec_comp(rec1, rec2)

                    if (cut_off_threshold == None) or (sum(w_vec) >= cut_off_threshold):
                        weight_vec_dict[(rec_ident1, rec_ident2)] = w_vec
                    else:
                        num_rec_pairs_below_thres += 1

        return [self.__get_field_names_list__(), weight_vec_dict]

    # ---------------------------------------------------------------------------

    def __get_index_values__(self, rec, data_set_num):

        index_var_values = []

        sep_str = self.index_sep_str

        assert (data_set_num == 0) or (data_set_num == 1)

        for index_def_list in self.index_def_proc:

            index_val_list = []

            for index_def in index_def_list:

                field_col = index_def[data_set_num]

                if (field_col >= len(rec)):
                    field_val = ''
                else:
                    field_val = rec[field_col].lower()

                if (field_val != ''):
                    if ((' ' in field_val) and (index_def[2] == True)):
                        word_list = field_val.split()
                        word_list.sort()
                        field_val = ' '.join(word_list)

                    if (index_def[3] == True):
                        field_val = field_val[::-1]

                    funct_def = index_def[5]

                    if (funct_def != None):
                        funct_call    =    funct_def[0]
                        num_funct_arg = len(funct_def)

                        if (num_funct_arg == 1):
                            funct_val = funct_call(field_val)
                        elif (num_funct_arg == 2):
                            funct_val = funct_call(field_val, funct_def[1])
                        elif (num_funct_arg == 3):
                            funct_val = funct_call(field_val, funct_def[1], funct_def[2])
                        elif (num_funct_arg == 4):
                            funct_val = funct_call(field_val, funct_def[1], funct_def[2], funct_def[3])
                        else:
                          logging.exception('Too many arguments for function call: %s' % \
                                            (str(funct_def)))
                          raise Exception
                    else:
                        funct_val = field_val

                        if (index_def[4] != None):
                            funct_val = funct_val[:index_def[4]]

                    index_val_list.append(funct_val)

            index_val = sep_str.join(index_val_list)

            index_var_values.append(index_val)

        assert len(index_var_values) == len(self.index_def_proc)

        return index_var_values

# =============================================================================

class BlockingIndex(Indexing):
    """Class that implements the 'classical' blocking used for record linkage.

     Records that have the same index variable values for an index are put into
     the same blocks, and only records within a block are then compared.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, **kwargs):

        Indexing.__init__(self, kwargs)

  # ---------------------------------------------------------------------------

    def build(self):

        self.__records_into_inv_index__()

        num_indices = len(self.index_def)

        self.num_rec_pairs = 0

        for i in range(num_indices):

            for block_val in self.index1[i]:
                block_num_recs = len(self.index1[i][block_val])

                self.num_rec_pairs += block_num_recs * (block_num_recs-1) / 2


        self.status = 'built'
    # ---------------------------------------------------------------------------

    def compact(self):

        num_indices = len(self.index_def)

        rec_pair_dict = {}

        for i in range(num_indices):

            this_index = self.index1[i]

            for block_val in this_index:

                block_recs = this_index[block_val]

                if (len(block_recs) > 1):

                    self.__dedup_rec_pairs__(block_recs, rec_pair_dict)


            self.index1[i].clear()  # Not needed anymore

            self.index2[i].clear()

            gc.collect()

        self.rec_pair_dict = rec_pair_dict

        self.num_rec_pairs = 0

        for rec_ident2_set in self.rec_pair_dict.itervalues():
            self.num_rec_pairs += len(rec_ident2_set)

        self.status = 'compacted'

      # ---------------------------------------------------------------------------

    def run(self, length_filter_perc = None, cut_off_threshold = None):

        # Compare the records
        #
        return self.__compare_rec_pairs_from_dict__(length_filter_perc, cut_off_threshold)

# =============================================================================

class SortingIndex(Indexing):
    """Class that implements the 'sorted neighbourhood' indexing approach based
         on an inverted index.
    """
    # ---------------------------------------------------------------------------

    def __init__(self, **kwargs):
        """Constructor. Process the 'window_size' argument first, then call the
           base class constructor.
        """

        self.window_size = None

        base_kwargs = {}

        for (keyword, value) in kwargs.items():

            if (keyword.startswith('window')):
                self.window_size = value
            else:
                base_kwargs[keyword] = value

        Indexing.__init__(self, base_kwargs)  # Initialise base class

    # ---------------------------------------------------------------------------

    def build(self):

        self.__records_into_inv_index__()  # Read records and put into index

        self.status = 'built'

    # ---------------------------------------------------------------------------

    def compact(self):
        """Method to compact an index data structure.
           Make a dictionary of all record pairs over all indices, which removes
           duplicate record pairs.
        """
        num_indices          = len(self.index_def)

        dedup_rec_pair_funct = self.__dedup_rec_pairs__

        window_size          = self.window_size

        rec_pair_dict = {}

        for i in range(num_indices):

            this_index = self.index1[i]

            w_block_len = [0] * window_size

            curr_window_recs = []  # List of record identifiers in current window

            block_val_list = this_index.keys()  # Get all blocking values

            block_val_list.sort()

            num_block_vals = len(block_val_list)

            for j in xrange(num_block_vals):

                window_j = j % window_size  # Modulo window size

                curr_window_recs = curr_window_recs[w_block_len[window_j]:]

                new_window_recs = set()

                for rec_ident in this_index[block_val_list[j]]:
                    if (rec_ident not in curr_window_recs):
                        new_window_recs.add(rec_ident)

                w_block_len[window_j] = len(new_window_recs)

                curr_window_recs += list(new_window_recs)

                if (len(curr_window_recs) > 1):

                    dedup_rec_pair_funct(curr_window_recs, rec_pair_dict)

            del curr_window_recs

        self.index1[i].clear()  # Not needed anymore
        self.index2[i].clear()

        gc.collect()

        self.rec_pair_dict = rec_pair_dict

        self.num_rec_pairs = 0

        for rec_ident2_set in self.rec_pair_dict.itervalues():
            self.num_rec_pairs += len(rec_ident2_set)

        self.status = 'compacted'

    # ---------------------------------------------------------------------------

    def run(self, length_filter_perc = None, cut_off_threshold = None):

        return self.__compare_rec_pairs_from_dict__(length_filter_perc, cut_off_threshold)


# =============================================================================

class SortingArrayIndex(Indexing):
    """Class that implements the 'sorted neighbourhood' indexing approach based
     on a sorted array.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, **kwargs):

        self.window_size = None

        base_kwargs      = {}

        for (keyword, value) in kwargs.items():
            if (keyword.startswith('window')):
                if (value == 1):
                  logging.exception('Window size must be larger than 1.')
                  raise Exception
                self.window_size = value
            else:
                base_kwargs[keyword] = value

        Indexing.__init__(self, base_kwargs)  # Initialise base class

    # ---------------------------------------------------------------------------
    def build(self):

        self.__records_into_inv_index__()  # Read records and put into index

    # ---------------------------------------------------------------------------

    def compact(self):

        num_indices = len(self.index_def)

        dedup_rec_pair_funct = self.__dedup_rec_pairs__

        window_size          = self.window_size

        rec_pair_dict = {}

        for i in range(num_indices):

            this_index = self.index1[i]  # Shorthand

            rec_sorted_array = []

            block_val_list = this_index.keys()

            block_val_list.sort()

            for block_key_val in block_val_list:
                rec_id_list = this_index[block_key_val]
                rec_sorted_array += rec_id_list

            # Can be shorter if empty blocking key values occur that
            #
            assert len(rec_sorted_array) <= self.dataset1.num_records

            # Now generate record pairs from the sliding window
            #
            for j in range(0, len(rec_sorted_array) - window_size + 1):

                # Get record identifiers in the current window
                #
                win_rec_id_list = rec_sorted_array[j:j+window_size][:]

                win_rec_id_list.sort()

                rec_cnt = 1

                for rec_ident1 in win_rec_id_list:

                    rec_ident2_set = rec_pair_dict.get(rec_ident1, set())

                    for rec_ident2 in win_rec_id_list[rec_cnt:]:
                        rec_ident2_set.add(rec_ident2)

                    rec_pair_dict[rec_ident1] = rec_ident2_set

                    rec_cnt += 1

            del rec_sorted_array


        self.index1[i].clear()  # Not needed anymore

        self.index2[i].clear()

        gc.collect()

        self.rec_pair_dict = rec_pair_dict

        self.num_rec_pairs = 0  # Count lengths of all record identifier sets - - -

        for rec_ident2_set in self.rec_pair_dict.itervalues():
            self.num_rec_pairs += len(rec_ident2_set)

        self.status = 'compacted'  # Update index status

    # ---------------------------------------------------------------------------

    def run(self, length_filter_perc = None, cut_off_threshold = None):

        return self.__compare_rec_pairs_from_dict__(length_filter_perc, cut_off_threshold)
