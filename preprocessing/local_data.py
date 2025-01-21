import csv

from shared import process_text_file
from columns import COLUMNS

if __name__ == "__main__":
    with open("./tichulog.txt") as f:
        rows = process_text_file(f.read(), "1")

    with open('./rows.csv', 'w', newline='') as f:
        write = csv.writer(f)
        write.writerow(COLUMNS.keys())
        write.writerows(rows)

    print("\n".join([",".join(map(str, row)) for row in rows]))