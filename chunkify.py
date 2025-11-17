import csv
import os

def split_csv(input_file, output_prefix, chunk_size=10):
    with open(input_file, mode='r', encoding='utf-8-sig') as infile:
        reader = csv.reader(infile)
        header = next(reader)

        file_count = 1
        row_count = 0

        outfile = open(f"{output_prefix}_part{file_count}.csv", mode='w', newline='', encoding='utf-8')
        writer = csv.writer(outfile)
        writer.writerow(header)

        for row in reader:
            # Clean each value in the row
            cleaned_row = [
                (col.strip().strip("'").strip('"') if col else "")
                for col in row
            ]

            if row_count >= chunk_size:
                outfile.close()
                file_count += 1
                row_count = 0

                outfile = open(f"{output_prefix}_part{file_count}.csv", mode='w', newline='', encoding='utf-8')
                writer = csv.writer(outfile)
                writer.writerow(header)

            writer.writerow(cleaned_row)
            row_count += 1

        outfile.close()
        print(f"Finished splitting. Created {file_count} files.")

split_csv("winnerprofile_email.csv", "output/winnerprofiles/chunk/chunk", chunk_size=50000) #change input file name and output prefix according to step1, step2 email/moble identifiers and chunk size