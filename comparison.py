# =============================================================================

"""Module with classes for field (attribute) and record comparisons.

   This module provides classes for record and field comparisons that can be
   used for the linkage process.
"""

# =============================================================================
# Import necessary modules (Python standard modules first, then Febrl modules)

import logging

# =============================================================================

class RecordComparator:
    """Class that implements a record comparator to compare two records and
     compute (and return) a weight vector.
    """
    # ---------------------------------------------------------------------------

    def __init__(self, dataset1, dataset2, field_comparator_list, descr = ''):

        self.description = descr

        self.dataset1 = dataset1

        self.dataset2 = dataset2

        self.field_comparator_list = field_comparator_list

        self.field_comparison_list = []  # Only compare methods and field columns

        # Extract field names from the two data set field name lists
        #
        dataset1_field_names = []

        dataset2_field_names = []

        for (field_name, field_data) in dataset1.field_list:
            dataset1_field_names.append(field_name)

        for (field_name, field_data) in dataset2.field_list:
            dataset2_field_names.append(field_name)

        # Go through field list and check if available
        #
        for (field_comp, field_name1, field_name2) in self.field_comparator_list:

            if (field_name1 not in  dataset1_field_names):
                logging.exception('Field "%s" is not in data set 1 field name list: ' % (field_name1) + '%s' % (str(dataset1.field_list)))
                raise Exception

            field_index1 = dataset1_field_names.index(field_name1)

            if (field_name2 not in dataset2_field_names):
                logging.exception('Field "%s" is not in data set 2 field name list: ' % (field_name2) + '%s' % (str(dataset2.field_list)))
                raise Exception

            field_index2 = dataset2_field_names.index(field_name2)

            field_tuple = (field_comp.compare, field_index1, field_index2)

            self.field_comparison_list.append(field_tuple)

        assert len(self.field_comparison_list) == len(self.field_comparator_list)

# =============================================================================

class FieldComparator:
    """Base class for field comparators.

        All field comparators have the following instance variables, which can be
        set when a field comparator is initialised:

        description      A string describing the field comparator.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, base_kwargs):
        """Constructor.
        """

        # General attributes for all field comparators
        #
        self.description     =  ''

        self.missing_values  =  ['']

        self.missing_weight  =  0.0

        self.agree_weight    =  1.0

        self.disagree_weight =  0.0

        # Process base keyword arguments (all data set specific keywords were
        # processed in the derived class constructor)
        #
        for (keyword, value) in base_kwargs.items():
            if (keyword.startswith('desc')):
                self.description = value

            else:
                logging.exception('Illegal constructor argument keyword: %s' % \
                          (str(keyword)))
                raise Exception

    # ---------------------------------------------------------------------------

    def __calc_freq_agree_weight__(self, val):

        return self.agree_weight

    # ---------------------------------------------------------------------------

    def __calc_freq_weights__(self, val1, val2):

        return self.agree_weight

    # ---------------------------------------------------------------------------
    def compare(self, val1, val2):
        """Compare two fields values, compute and return a numerical weight. See
        implementations in derived classes for details.
        """
        logging.exception('Override abstract method in derived class')
        raise Exception

# =============================================================================


class FieldComparatorExactString(FieldComparator):
    """A field comparator based on exact string comparison.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, **kwargs):
        """No additional attributes needed, just call base class constructor.
        """

        FieldComparator.__init__(self, kwargs)

    # ---------------------------------------------------------------------------

    def compare(self, val1, val2):
        """Compare two field values using exact string comparator.
        """
        if (val1 in self.missing_values) or (val2 in self.missing_values):
            return self.missing_weight
        elif val1 == val2:
            return self.__calc_freq_agree_weight__(val1)
        else:
            return self.disagree_weight

# =============================================================================

class FieldComparatorApproxString(FieldComparator):

    # ---------------------------------------------------------------------------

    def __init__(self, kwargs):

        self.threshold = None

        # Process all keyword arguments - - - - - - - - - - - - - - - - - - - - - -
        #
        base_kwargs = {}  # Dictionary, will contain unprocessed arguments for base
                      # class constructor

        for (keyword, value) in kwargs.items():
            if (keyword.startswith('thres')):
                if (value == 1.0):
                    logging.exception('Value of argument "threshold" must be smaller' + \
                            'then 1.0: %s' % (value))
                    raise Exception
                self.threshold = float(value)
            else:
                base_kwargs[keyword] = value

        FieldComparator.__init__(self, base_kwargs)  # Process base arguments

    # ---------------------------------------------------------------------------

    def __calc_partagree_weight__(self, val1, val2, approx_sim_val):
        """Calculate the partial agreement weight. Should not be used from outside
            the module.
        """

        # Check if similarity values is below or above threshold
        #
        if (approx_sim_val < self.threshold):
            return self.disagree_weight

        # Get general or frequency based agreement weight
        #
        agree_weight = self.__calc_freq_weights__(val1, val2)

        # Compute final adjusted weight - - - - - - - - - - - - - - - - - - - - -

        return agree_weight-(1.0-approx_sim_val) / (1.0-self.threshold) * \
           (agree_weight + abs(self.disagree_weight))


# =============================================================================

class FieldComparatorJaro(FieldComparatorApproxString):
    """A field comparator based on the Jaro approximate string comparator.
    """

    # ---------------------------------------------------------------------------

    def __init__(self, **kwargs):
        """No additional attributes needed, just call base class constructor.
        """
        FieldComparatorApproxString.__init__(self, kwargs)

        self.JARO_MARKER_CHAR = chr(1)  # Special character used to mark assigned
                                    # characters

    # ---------------------------------------------------------------------------

    def compare(self, val1, val2):
        """Compare two field values using the Jaro approximate string comparator.
        """

        # Check if one of the values is a missing value
        #
        if (val1 in self.missing_values) or (val2 in self.missing_values):
            return self.missing_weight


        if (val1 == val2):
            return self.__calc_freq_agree_weight__(val1)

        # Calculate Jaro similarity value - - - - - - - - - - - - - - - - - - - - -
        #
        len1, len2 = len(val1), len(val2)

        halflen = max(len1, len2) / 2 - 1

        ass1, ass2 = '', ''  # Characters assigned in string 1 and string 2

        workstr1, workstr2 = val1, val2  # Copies of the original strings

        common1, common2 = 0.0, 0.0  # Number of common characters

        for i in range(len1):  # Analyse the first string
            start = max(0,i-halflen)
            end   = min(i+halflen+1,len2)
            index = workstr2.find(val1[i],start,end)
            if (index > -1):  # Found common character
                common1 += 1
                ass1 = ass1 + val1[i]
                workstr2 = workstr2[:index]+self.JARO_MARKER_CHAR+workstr2[index+1:]

        for i in range(len2):  # Analyse the second string
            start = max(0,i-halflen)
            end   = min(i+halflen+1,len1)
            index = workstr1.find(val2[i],start,end)
            if (index > -1):  # Found common character
                common2 += 1
                ass2 = ass2 + val2[i]
                workstr1 = workstr1[:index]+self.JARO_MARKER_CHAR+workstr1[index+1:]

        assert (common1 == common2), 'Jaro: Different "common" values'

        if (common1 == 0.0):  # No characters in common
            w = self.disagree_weight
        else:  # Compute number of transpositions  - - - - - - - - - - - - - - - -

            transp = 0.0
            for i in range(len(ass1)):
                if (ass1[i] != ass2[i]):
                    transp += 0.5

            w = 1./3.*(common1 / float(len1) + common1 / float(len2) + (common1-transp) / common1)

            assert (w > 0.0), 'Jaro: Weight is smaller than 0.0: %f' % (w)

            assert (w < 1.0), 'Jaro: Weight is larger than 1.0: %f' % (w)

            w = self.__calc_partagree_weight__(val1, val2, w)

        if (self.do_caching == True):  # Put values pair into the cache
            self.__put_into_cache__(val1, val2, w)

        return w

# =============================================================================


