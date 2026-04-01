import OLAP_system.database as database, OLAP_system.sql_parser as sql_parser

def exec_sql(data):
    sql_parser.parse(data)

def main():
    print("SQL repl tester")
    input_str = ""
    while True:
        if input_str == "":
            input_str += input("> ").strip()
            if input_str == "\\exit":
                break
            elif input_str == "\\print_all":
                database.print_summary()
                input_str = ""
                continue
        else:
            input_str += input(". ")
        input_str_arr = input_str.split(";")
        while len(input_str_arr) >= 2:
            exec_sql(input_str_arr[0] + ";")
            input_str_arr = input_str_arr[1:]
        input_str = input_str_arr[0]
        if len(input_str_arr[0]) != 0:
            input_str += " "

if __name__ == "__main__":
    main()