import sys
import boto3

PRINT_SPACES = ' '*6
_first_print_flag = True


def parse_ddl(p_table):
    global _first_print_flag

    glue_client = boto3.client('glue')
    db_name, table_name = p_table.split('.')
    attr_list = glue_client.get_table(DatabaseName=db_name,
                                      Name=table_name)['Table']['StorageDescriptor']['Columns']
    print('SELECT')
    for col in attr_list:
        if col['Type'][0:6] == 'struct' or col['Type'][0:5] == 'array':
            parse_struct(col['Name']+':'+col['Type'])
        else:
            print(PRINT_SPACES, '' if _first_print_flag else ',', col['Name'])
            _first_print_flag = False if _first_print_flag else False
    print('  FROM {} \n  LIMIT 10 \n;'.format(p_table))


def parse_struct(p_struct_str):
    parent_list = []
    for pattern, delim in next_pattern(p_struct_str):
        pattern_list = pattern.split(':')
        if len(pattern_list) == 2:
            if pattern_list[1] in ('struct', 'array'):
                parent_list.append(pattern_list[0])
                if pattern_list[1] == 'array':
                    parent_list[-1] += '[1]'
            else:
                print_elem(parent_list, pattern_list[0])
        else:  # len(pattern_list) < 2
            if pattern_list[0] == 'struct':
                parent_list.append('')
            elif pattern_list[0] != '':
                print_elem(parent_list, '')
        if delim == '>':
            parent_list.pop()


def print_elem (p_parent_list, p_elem):
    global _first_print_flag
    parent_prefix = p_parent_list[0]
    for name in p_parent_list[1:]:
        if name != '':
            parent_prefix += '.' + name

    print(PRINT_SPACES, '' if _first_print_flag else ',', parent_prefix+('.'+p_elem if p_elem != '' else ''))
    _first_print_flag = False if _first_print_flag else False


def next_pattern (p_struct_str):
    delim_list = [',', '<', '>']
    curr_delim = ''
    start_pos = 0
    #    ctr = 0
    while start_pos < len(p_struct_str):
        delim_pos = 999999
        for delim in delim_list:
            curr_delim_pos = p_struct_str.find(delim, start_pos)
            if curr_delim_pos < delim_pos and curr_delim_pos != -1:
                delim_pos = curr_delim_pos
                curr_delim = delim
        ret_str = p_struct_str[start_pos:delim_pos]
        start_pos = delim_pos + 1
        yield ret_str, curr_delim


if __name__ == '__main__':
    parse_ddl(sys.argv[1])
