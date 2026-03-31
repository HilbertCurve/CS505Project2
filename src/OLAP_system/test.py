"""Entry point for all test cases in this DBMS project.
"""
import CS505Project2.src.OLAP_system.sql_parser as sql_parser

def run_tests():
    print("Tokenizer tests... ")
    sql_code = 'CREATE TABLE foo ( col_name INTEGER, col_name2 VARCHAR(100), );'
    sql_code_parsed = [
        'CREATE', 'TABLE', 'foo', '(',
        'col_name', 'INTEGER', ',',
        'col_name2', 'VARCHAR', '(', '100', ')', ',',
        ')', ';'
    ]
    sql_load = 'LOAD csvfile.csv INTO foo;'
    sql_load_parsed = ['LOAD', 'csvfile.csv', 'INTO', 'foo', ';']
    assert sql_parser.tokenize(sql_code) == sql_code_parsed
    assert sql_parser.tokenize(sql_load) == sql_load_parsed
    print("\tpassed.")

if __name__ == "__main__":
    run_tests()

