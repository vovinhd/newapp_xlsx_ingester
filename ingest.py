import io
from tokenize import group
from numpy import diff
import pandas as pd

xl_file = "./Challenges.xlsx"
table = "Tabelle1"
index_col = "Titel der Challenge "
merge_root = "Schwierigkeitsgrade"
merge_cols = [
    "Schwierigkeitsgrad/Aufgabenbeschreibung/Checkliste", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6"]

# parse the checklist string to a list of strings


def parse_checklist(checklist):
    _checklist = []
    for item in checklist.split("&#10;"):
        _checklist.append(item.strip())
    return _checklist


if __name__ == "__main__":
    dfs = pd.read_excel(xl_file, sheet_name=None)

    # dfs = pd.read_csv("./test.csv", encoding='utf-8')
    selected_df = dfs[table]

    selected_df.reset_index()

    # combine all rows of a challenge
    current_challenge = -1
    for index, row in selected_df.iterrows():
        if not pd.isna(row[index_col]):
            current_challenge = index
        else:
            for merge_col in merge_cols:

                current_val = selected_df.at[current_challenge, merge_col]
                # check if current_val is an array-like
                if isinstance(current_val, list):

                    current_val.append(
                        row[merge_col])
                else:
                    current_val = [
                        current_val, row[merge_col]]

                selected_df.at[current_challenge, merge_col] = current_val
    selected_df.reset_index()

    # drop redundant rows
    for index, row in selected_df.iterrows():
        if pd.isna(row[index_col]):
            # delete row
            selected_df.drop(index, inplace=True)

    # create difficulty objects per challenge

    difficulty = []
    for index, row in selected_df.iterrows():
        current_difficulties = {}
        for merge_col in merge_cols:
            head, *tail = row[merge_col]
            # if head is string replace &#10; with :
            if isinstance(head, str):
                head, *s = head.split("&#10;")
            else:
                continue
            print(tail)
            opts = {
                "Aufgabenbeschreibung": tail[0], "Checkliste": parse_checklist(tail[1]) if len(tail) > 1 else None, "hint": s[0] if s != [] else None}
            current_difficulties[head] = opts
        difficulty.append(current_difficulties)

    selected_df["Schwierigkeitsgrade"] = difficulty

    selected_df = selected_df.drop(merge_cols, 1)

    out = selected_df.to_json(
        orient='records', indent=True, force_ascii=False)
    # out = out.encode('utf-8')

    with io.open("text.json", mode="w", encoding="UTF8") as fd:
        fd.writelines(out)
