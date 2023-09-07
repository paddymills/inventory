
from lib.db import SndbConnection

import os
import xlwings


JOB = '1210027E'
SHIPMENT = 7

class Program:
    def __init__(self):
        self.parts = list()

    def add_part(self, part):
        self.parts.append(part)

    @property
    def total_parts_qty(self):
        return sum([part.qty for part in self.parts])


class Part:
    def __init__(self, name, qty):
        self.name = name
        self.qty = qty

def main():
    # set directory to script root
    os.chdir(os.path.dirname(__file__))

    with SndbConnection() as db:
        # get data
        # these scripts may seem overly complicated. they were originally
        # created to dump data from SQL to excel without python as the middleman

        programs = dict()
        parts_on_program = db.query_from_sql_file(r'sql\portal_drops_parts.sql')
        for prog, part, qty in parts_on_program:
            prog = int(prog)
            if prog not in programs:
                programs[prog] = Program()

            programs[prog].add_part(Part(part, qty))

        result = [None]
        program_data = db.query_from_sql_file(r'sql\portal_drops_programs.sql')
        for row in program_data:
            program = programs[int(row.ProgramName)]


            total_used = row.UsedArea
            total_scrap = row.ScrapFraction * total_used
            total_scrapweight = total_scrap * row.SheetThickness * 0.2836
            for part in program.parts:
                part_qty_multiplier = part.qty / program.total_parts_qty

                result.append([
                    row.MaterialGroup,
                    row.MaterialMaster,
                    row.ProgramName,
                    part.name,
                    part.qty,
                    row.StockType,
                    row.SheetThickness,
                    row.SheetWidth,
                    row.SheetLength,
                    row.SheetArea,
                    total_used * part_qty_multiplier,
                    row.ScrapFraction,
                    total_scrap * part_qty_multiplier,
                    total_scrapweight * part_qty_multiplier,
                ])
        else:
            result[0] = [t[0] for t in row.cursor_description]

    # write data
    wb = xlwings.Book(r'templates\Portal_drop_weights-template.xlsx')
    wb.sheets.active.range("A1").value = result


if __name__ == '__main__':
    main()
