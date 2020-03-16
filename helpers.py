from collections import MutableMapping
import re
import json
import uuid

def get_keys(bucket):
    ret = []
    for obj in bucket.objects.all():
        ret.append(obj.key)
    return ret

def flatten_dict(d, parent_key ='', sep ='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep = sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def format_column(header):
    return re.sub( '(?<!^)(?=[A-Z])', '_', header ).lower()

def random_file_names(suffix, length, f_type='.csv'):
    '''
    adds randomness to prefix of s3 file
    s3 takes the prefix of the file and maps it onto a partitions
    same prefix means same partition, and will hurt performance
    random prefix ensures unique partions and efficient performance
    '''
    file_names = []
    for x in range(length):
        file_names.append(''.join([str(uuid.uuid4().hex[:6]), suffix])+f_type)
    return file_names
