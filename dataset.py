
# =============================================================================

import csv
import logging
import string


# =============================================================================

class DataSet:

    def __init__(self, base_kwargs):
        """Constructor, set general attributes.
        """

        self.type           = ''
        self.description    = ''
        self.access_mode    = None
        self.field_list     = None
        self.rec_ident      = None
        self.strip_fields   = True
        self.miss_val       = None
        self.num_records    = None

        for (keyword, value) in base_kwargs.items():

            if (keyword.startswith('desc')):
                self.description = value

            elif (keyword.startswith('access')):
                if (value not in ['read','write','append','readwrite']):
                    logging.exception('Illegal "access_mode" value: %s' % (str(value)))
                    raise Exception
                self.access_mode = value

            elif (keyword.startswith('field_l')):
                self.field_list = value

            elif (keyword.startswith('rec_id')):
                self.rec_ident = value

            elif (keyword.startswith('strip_f')):
                self.strip_fields = value

            elif (keyword.startswith('miss_v')):
                self.miss_val = value
            else:
                logging.exception('Illegal constructor argument keyword: "%s"' % \
                    (str(keyword)))
                raise Exception

        if (self.miss_val != None):
            clean_miss_val = []

            for miss_val in self.miss_val:
                stripped_miss_val = miss_val.strip()
                if (stripped_miss_val != ''):  # Not empty string
                    clean_miss_val.append(stripped_miss_val)

            if (clean_miss_val != []):
                self.miss_val = clean_miss_val

            else:
                self.miss_val = None

  # ---------------------------------------------------------------------------

class DataSetCSV(DataSet):

    def __init__(self, **kwargs):
        """Constructor. Process the derived attributes first, then call the base
         class constructor.
        """

        self.dataset_type     = 'CSV'

        self.file_name        = None

        self.header_line      = False

        self.file             = None

        self.delimiter        = ','

        self.next_rec_num     = None

        self.rec_ident_col    = -1

        # Process all keyword arguments
        #

        base_kwargs = {}  # Dictionary, will contain unprocessed arguments

        for (keyword, value) in kwargs.items():

            if (keyword.startswith('file')):
                self.file_name = value

            elif (keyword.startswith('header')):
                self.header_line = value

            elif (keyword.startswith('delimi')):
                self.delimiter = value

            else:
                base_kwargs[keyword] = value

        DataSet.__init__(self, base_kwargs)  # Process base arguments

        if ((self.header_line == False) and (self.field_list == None)):
            logging.exception('Argument "field_list" must be given if field ' + \
                            'names are not taken from header line')
            raise Exception

        if ((self.header_line == True) and (self.access_mode == 'read')):
            self.field_list = []  # Will be generated from file header line


        if (self.access_mode == 'read'):

            try:  # Try to open the file in read mode
                self.file = open(self.file_name,'r')
            except:
                logging.exception('Cannot open CSV file "%s" for reading' % (self.file_name))
                raise IOError

            # Initialise the CSV parser - - - - - - - - - - - - - - - - - - - - - - -
            #

            self.csv_parser = csv.reader(self.file, delimiter = self.delimiter)

            # If header line is set to True get field names
            #
            if (self.header_line == True):
                header_line     = self.csv_parser.next()

                self.field_list = []

                col_num = 0

                for field_name in header_line:
                    if (self.strip_fields == True):
                        field_name = field_name.strip()
                    self.field_list.append((field_name,col_num))
                    col_num += 1

            num_rows = 0

            fp = open(self.file_name,'r')

            for l in fp:
                num_rows += 1

            fp.close()

            self.num_records = num_rows

            if (self.header_line == True):
                self.num_records -= 1

            # Check that there are records in the data set
            #
            if (self.num_records == 0):
                logging.exception('No records in CSV data set opened for reading')
                raise Exception

            self.next_rec_num = 0

        else:  # Illegal data set access mode - - - - - - - - - - - - - - - - - - -

            logging.exception('Illegal data set access mode: "%s" (not currently allowed / supported ' % \
                (str(self.access_mode)) + 'with CSV data set implementation).')
            raise Exception

        this_col_num = 0  # Check if column numbers are consecutive

        for (field_name, field_col) in self.field_list:

            if (this_col_num != field_col):
                logging.exception('Column numbers are not consecutive: %s' % (str(self.field_list)))
                raise Exception
            else:
                this_col_num += 1

            # Check if this is the record identifier field
            #
            if (self.rec_ident == field_name):
                self.rec_ident_col = field_col

  # ---------------------------------------------------------------------------

    def readall(self):
        """An iterator which will return one record per call as a tuple (record
           identifier, record field list).

           The file is first closed and then re-opened returning the first record.
        """

        if (self.file == None):
            logging.exception('Data set not initialised')
            raise Exception

        if (self.access_mode != 'read'):
            logging.exception('Data set not initialised for "read" access')
            raise Exception

        self.file.close()

        self.file = open(self.file_name,'r')

        self.next_rec_num = 0

        # Initialise the CSV parser as reader
        #
        self.csv_parser = csv.reader(self.file, delimiter = self.delimiter)

        # Skip over header (if there is one) and skip to start record
        #
        if (self.header_line == True):
            self.csv_parser.next()

        for rec in self.csv_parser:

            if (self.strip_fields == True):
                rec = map(string.strip, rec)

            if (self.miss_val != None):
                clean_rec = []

                miss_val_list = self.miss_val

                for val in rec:
                    if (val in miss_val_list):
                        clean_rec.append('')
                    else:
                        clean_rec.append(val)

                rec = clean_rec

            if (self.rec_ident_col == -1):
                rec_ident = self.rec_ident+'-%d' % (self.next_rec_num)

            else:
                rec_ident = rec[self.rec_ident_col]

            self.next_rec_num += 1

            yield (rec_ident,rec)


# =============================================================================
