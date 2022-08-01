import io
import os
import string
import pandas as pd
import re
import argparse
import warnings
import json
from PIL import Image
from pathlib import Path
warnings.simplefilter(action='ignore', category=UserWarning)
xl_file = "./Challenges.xlsx"
tables_to_ignore = ["Übersicht"]
index_col = "Titel der Challenge "
merge_root = "Schwierigkeitsgrade"
merge_cols = [
    "Schwierigkeitsgrad/Aufgabenbeschreibung/Checkliste", "Unnamed: 7", "Unnamed: 8"]
tag_col = "Tags"
drop_cols = ["Themenmonat", "Themenmonat 2"]
out_file = "text.json"
image_col = "Bild"
image_base_dir = "./Bilder/"
image_out_dir = "./images/"
verbose = False
# parse the checklist string to a list of strings

tags = {}
topics = {}


def collect_tags(tag_string):
    tags_out = []

    tag_arr = tag_string.split("\n")
    for tag in tag_arr:
        tag_text = tag.strip()
        tag = make_slug(tag)
        tags_out.append(tag)
        tags[tag] = tag_text
    return tags_out


def find_image(name):
    print("lookup image: " + name)
    print(os.listdir(image_base_dir))
    for file in os.listdir(image_base_dir):
        if file.startswith(name):
            return file
    return None


def convert_image(source):
    destination = Path(image_out_dir + source).with_suffix(".webp")
    print("opening: " + str(source) + " to: " + str(destination))
    image = Image.open(image_base_dir + source)  # Open image
    image.save(destination, format="webp")  # Convert image to webp

    return destination.as_posix(), image.size


def make_image_dict(image_string):
    image_dict = {}
    image_arr = image_string.split("\n")
    for source in image_arr:
        source = source.strip()
        if source == "":
            continue
        if source.startswith("http"):
            image_dict["url"] = source
        else:

            path = find_image(source)
            if path is None:
                print("Image not found: " + image_base_dir + source)
                continue
            else:
                dest, (x, y) = convert_image(path)
                image_dict["file"] = {
                    "path": dest,
                    "size": {"x": x, "y": y}
                }
        # image_dict[image.strip()] = True
    return image_dict


def parse_checklist(checklist):
    _checklist = []
    for item in re.split("&#10;|\n", checklist):
        todo = item
        todo = re.sub(r"\*", " ", todo)
        todo = todo.strip()
        _checklist.append({"name": todo, "reward": None})
    return _checklist


def log(msg, *args):
    if verbose:
        print(msg, *args)


def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-i', '--input', help='input file',
                        default=xl_file)
    parser.add_argument(
        '-o', '--output', help='output file', default=out_file)

    parser.add_argument('-t', '--tables', help='table name',
                        default=tables_to_ignore)
    parser.add_argument('-c', '--index', help='index column',
                        default=index_col)
    parser.add_argument(
        '-m', '--merge', help='merge columns', default=merge_cols)

    parser.add_argument('-d', '--drop', help='drop columns',
                        default=drop_cols)
    parser.add_argument('-g', '--tag', help='tag column', default=tag_col)
    parser.add_argument(
        '-r', '--root', help='root of merge', default=merge_root)

    parser.add_argument('-v', '--verbose', help='verbose', action='store_true')
    parser.add_argument(
        '-im', '--image', help='image base dir', default=image_base_dir)
    parser.add_argument('-imd', '--image-dir',
                        help='image output dir', default=image_out_dir)
    parser.add_argument('-imc', '--image-col',
                        help='image column', default=image_col)
    args = parser.parse_args()
    return args


def make_slug(title):
    slug = title.lower().strip()
    slug = re.sub(r" ", "_", slug)
    # replace umlaute in slug
    slug = re.sub(r"ä", "ae", slug)
    slug = re.sub(r"ö", "oe", slug)
    slug = re.sub(r"ü", "ue", slug)
    slug = re.sub(r"ß", "ss", slug)
    return re.sub(r"[^a-zA-Z0-9\_]+", "", slug)


def parseSheet(name, dataframes, current_topic) -> pd.DataFrame:
    selected_df = dataframes[name]
    selected_df.reset_index()

    # log(selected_df.columns)

    # combine all rows of a challenge
    current_challenge = -1
    for index, row in selected_df.iterrows():
        if not pd.isna(row[index_col]):
            current_challenge = index
            log("✏ Adding challenge:", row[index_col])
        else:
            if current_challenge == -1:
                # log("Skipping head row:", index)
                continue
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

            image_str = selected_df.at[current_challenge, image_col]
            log(image_str)
            if isinstance(image_str, str):
                log("🖼 Adding image:", image_str)
                image_data = make_image_dict(image_str)
                selected_df.at[current_challenge, image_col] = image_data
    selected_df.reset_index()

    # drop redundant rows
    for index, row in selected_df.iterrows():
        if pd.isna(row[index_col]):
            # delete row
            selected_df.drop(index, inplace=True)

    # create difficulty objects per challenge
    selected_df.reset_index()
    difficulty = []
    for index, row in selected_df.iterrows():
        current_difficulties = {}
        for merge_col in merge_cols:
            head, *tail = merge_col, row[merge_col]
            if "Schwierigkeitsgrad/Aufgabenbeschreibung/Checkliste" == head:
                head = "easy"
            elif "Unnamed: 7" == head:
                head = "medium"
            elif "Unnamed: 8" == head:
                head = "hard"
            else:
                log("Unknown difficulty:", head)

            # if head is string replace &#10; with :
            if isinstance(head, str):
                # &#10; is excels representation of a newline
                head, *s = head.split("&#10;")
            else:
                continue

            tail = tail[0]
            if not isinstance(tail[0], str):
                continue
            opts = {
                "taskDescription": tail[0],
                "todos": parse_checklist(tail[1]) if len(tail) > 1 else None,
            }
            current_difficulties[head] = opts
        difficulty.append(current_difficulties)

    # add a col to dataframe with the difficulty objects
    selected_df["Schwierigkeitsgrade"] = difficulty

    # create slug column for each challenge in selected_df by copying the index_col and replacing spaces with underscores
    selected_df["slug"] = list(
        map(lambda x: make_slug(x), selected_df[index_col]))
    # remove all merged columns
    selected_df = selected_df.drop(columns=merge_cols, axis=1)
    selected_df = selected_df.drop(columns=drop_cols, axis=1)

    # split tag column into array of tags
    for index, row in selected_df.iterrows():
        selected_df.at[index, tag_col] = collect_tags(row[tag_col])

    selected_df.rename(columns={
        'Titel der Challenge ': 'title',
        'Impact ': 'impact',
        'Tags': 'tags',
        'Beschreibung': 'frontMatter',  # todo
        'Hintergrundwissen/ Infobytes': 'content',
        'Schwierigkeitsgrade': 'difficulties',
        'Themenmonat': "topic",
        'Wiederkehrende Challenge wöchentlich (Ja/Nein)': 'weekly',
        'Punkte (alte App)': 'points',
        'Bild': 'image',

    }, inplace=True)

    for index, row in selected_df.iterrows():
        selected_df.at[index, "topic"] = current_topic

    selected_df.reset_index()
    return selected_df


def main():
    dfs = pd.read_excel(xl_file, sheet_name=None)

    # dfs = pd.read_csv("./test.csv", encoding='utf-8')

    for table in dfs.keys():
        if table in tables_to_ignore:  # skip tables that are not challenges
            continue
        current_topic = make_slug(table)
        topics[current_topic] = table.strip()
        log("  Adding topic", table, "as", current_topic)
        dfs[table] = parseSheet(table, dfs, current_topic)
        log("✔ topic", table, "done")
    # convert to json
    json_dict = {
        "tags": tags,
        "topics": topics,
        "challenges": {},
    }
    for table in dfs.keys():
        if table in tables_to_ignore:  # skip tables that are not challenges
            continue
        log("Converting table:", table)
        # frame_json = dfs[k].to_json(
        #     f"{k}.json", orient="records", indent=True, force_ascii=False)
        # frame_json = dfs[table].to_json(
        #     orient="records", indent=True, force_ascii=False)
        frame_json = dfs[table].to_dict(orient="records")
        # re.sub(r'\\/', '/', frame_json)
        json_dict["challenges"][table] = frame_json

    log("Converting to json done")
    # log(json_dict)
    out = json.dumps(json_dict, indent=4, ensure_ascii=False)
    out = re.sub(r'\\/', '/', out)
    out = re.sub(r'NaN', '\"\"', out)

    # write to file
    with io.open(out_file, mode="w", encoding="UTF8") as fd:
        log("Writing to file:", out_file)
        fd.writelines(out)


if __name__ == "__main__":
    args = parse_args()
    xl_file = args.input
    tables_to_ignore = args.tables
    index_col = args.index
    merge_root = args.root
    merge_cols = args.merge
    tag_col = args.tag
    drop_cols = args.drop
    out_file = args.output
    verbose = args.verbose
    main()
