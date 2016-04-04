# =============================================================================
"""Module encode.py - Various phonetic name encoding methods.

Encoding methods provided:

  nysiis          NYSIIS

  get_substring   Simple function which extracts and returns a sub-string

Note that all encoding routines assume the input string only contains letters
and whitespaces, but not digits or other ASCII characters.
"""

# =============================================================================
# Imports go here

import logging
import string
import time

# =============================================================================

def nysiis(s, maxlen=4):
    """Compute the NYSIIS code for a string.
    """

    if (not s):
        return ''

    # Remove trailing S or Z
    #
    while s and s[-1] in 'sz':
        s = s[:-1]

    # Translate first characters of string
    #
    if (s[:3] == 'mac'):  # Initial 'MAC' -> 'MC'
        s = 'mc'+s[3:]
    elif (s[:2] == 'pf'):  # Initial 'PF' -> 'F'
        s = s[1:]

    # Translate some suffix characters:
    #
    suff_dict = {'ix':'ic', 'ex':'ec', 'ye':'y', 'ee':'y', 'ie':'y', 'dt':'d', 'rt':'d', 'rd':'d', 'nt':'n', 'nd':'n'}

    suff = s[-2:]

    s = s[:-2]+suff_dict.get(suff, suff)

    # Replace EV with EF
    #

    if (s[2:].find('ev') > -1):
        s = s[:-2]+s[2:].replace('ev','ef')

    if (not s):
        return ''

    first = s[0]  # Save first letter for final code

    # Replace all vowels with A and delete whitespaces
    #
    voweltable = string.maketrans('eiou', 'aaaa')

    s2 = string.translate(s,voweltable, ' ')

    if (not s2):  # String only contained whitespaces
        return ''

    # Remove all W that follow an A
    #
    s2 = s2.replace('aw','a')

    # Various replacement patterns
    #
    s2 = s2.replace('ght','gt')
    s2 = s2.replace('dg','g')
    s2 = s2.replace('ph','f')
    s2 = s2[0]+s2[1:].replace('ah','a')
    s3 = s2[0]+s2[1:].replace('ha','a')
    s3 = s3.replace('kn','n')
    s3 = s3.replace('k','c')
    s4 = s3[0]+s3[1:].replace('m','n')
    s5 = s4[0]+s4[1:].replace('q','g')
    s5 = s5.replace('sh','s')
    s5 = s5.replace('sch','s')
    s5 = s5.replace('yw','y')
    s5 = s5.replace('wr','r')

    # If not first or last, replace Y with A
    #
    s6 = s5[0]+s5[1:-1].replace('y','a')+s5[-1]

    # If not first character, replace Z with S
    #
    s7 = s6[0]+s6[1:].replace('z','s')

    # Replace trailing AY with Y
    #
    if (s7[-2:] == 'ay'):
        s7 = s7[:-2]+'y'

    # Remove trailing vowels (now only A)
    #
    while s7 and s7[-1] == 'a':
        s7 = s7[:-1]

    if (len(s7) == 0):
        resstr = ''
    else:
        resstr = s7[0]
        for i in s7[1:]:
            if (i != resstr[-1]):
                resstr=resstr+i

    # Now compile final result string
    #
    if (first in 'aeiou'):
        resstr = first+resstr[1:]

    if (maxlen > 0):
        resstr = resstr[:maxlen]  # Return first maxlen characters

    return resstr

# =============================================================================

def get_substring(s, start_index, end_index):
    """Simple function to extract and return a substring from the given input
     string.
    """
    assert start_index <= end_index

    return s[start_index:end_index]
