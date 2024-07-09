from pathlib import Path
import os
import re
import json
import pandas as pd
from datetime import datetime, timezone
import matplotlib.pyplot as plt

DATA_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "results"

PATH_REGEX = re.compile(r"(?P<engine_name>("
                        r"?P<engine>[a-z0-9\.\-]+)"
                        r"\-m\-(?P<m>[0-9]+)"
                        r"\-ef\-(?P<ef>[0-9]+)"
                        r")"
                        r"\-(?P<dataset>[a-zA-Z0-9\-]+)"
                        r"\-(?P<operation>(search)|(upload))"
                        r"(\-(?P<search_index>[0-9]{1,2})\-)?"
                        r"\-?(?P<date>.*)\.json")

def load_results():
    upload_results, search_results = {}, {}

    for path in DATA_DIR.glob("*.json"):
        match = PATH_REGEX.match(path.name)
        if match is None:
            continue
        
        experiment = match.groupdict()
        engine = match["engine"]
        dataset = match["dataset"]
        if engine not in search_results:
            search_results[engine] = dict()
        if engine not in upload_results:
            upload_results[engine] = dict()
     
        with open(path, "r") as fp:
            stats = json.load(fp)

        if experiment["operation"] == "search":
            parallel = stats["params"]["parallel"]
            if parallel not in search_results[engine]:
                search_results[engine][parallel] = dict()
            if dataset not in search_results[engine][parallel]:
                search_results[engine][parallel][dataset] = []

            entry = [match["m"], match["ef"],
                     stats["results"]["mean_precisions"],
                     stats["results"]["rps"],
                     stats["results"]["mean_time"],
                     stats["results"]["p95_time"],
                     stats["results"]["p99_time"]]
            search_results[engine][parallel][dataset].append(list(map(str, entry)))
        elif experiment["operation"] == "upload":
            if dataset not in upload_results[engine]:
                upload_results[engine][dataset] = []

            entry = [match["m"], match["ef"],
                     stats["results"]["upload_time"],
                     stats["results"]["total_time"]]
            upload_results[engine][dataset].append(list(map(str, entry)))

    return (upload_results, search_results)


def save_search_results(search_results):
    files = {}
    for engine, engine_data in search_results.items():
        for parallel, parallel_data in engine_data.items():
            for dataset, dataset_data in parallel_data.items():
                if dataset not in files:
                    files[dataset] = {}
                if parallel not in files[dataset]:
                    files[dataset][parallel] = {}
                filename = f"data-search-{engine}-{parallel}-{dataset}.csv"
                filepath = DATA_DIR / filename
                with open(filepath, "w") as f:
                    f.write("m,ef,mean_precisions,rps,mean_time,p95_time,p99_time\n")
                    for row in dataset_data:
                        f.write(",".join(row))
                        f.write("\n")
                files[dataset][parallel][engine] = filepath
    return files


def save_upload_results(upload_results):
    files = {}
    for engine, engine_data in upload_results.items():
        for dataset, dataset_data in engine_data.items():
            if dataset not in files:
                files[dataset] = {}
            filename = f"data-upload-{engine}-{dataset}.csv"
            filepath = DATA_DIR / filename
            with open(filepath, "w") as f:
                f.write("m,ef,upload_time,total_time\n")
                for row in dataset_data:
                    f.write(",".join(row))
                    f.write("\n")
            files[dataset][engine] = filepath
    return files


def gen_charts_search_results(files):

    plt.style.use('default')

    for dataset, dataset_data in files.items():
        for parallel, parallel_data in dataset_data.items():
            # New RPS chart
            fig = plt.figure(figsize=(10, 5), dpi=150)
            data = None
            for engine, filepath in parallel_data.items():
                data = pd.read_csv(filepath, index_col='mean_precisions')
                data["rps"].sort_index().plot(label=engine)

            plt.title(f"Search RPS - {dataset} - {parallel} threads")
            plt.xlabel("Precision")
            plt.ylabel("RPS")
            plt.grid(visible=True, which='major', axis='both', color='#F5F5F5', linestyle='--')
            plt.ylim(bottom=0)
            plt.legend()

            fig.savefig(DATA_DIR / f"data-rps-{dataset}-{parallel}.png")
            # New lentcy chart
            fig = plt.figure(figsize=(10, 5), dpi=150)
            data = None
            for engine, filepath in parallel_data.items():
                data = pd.read_csv(filepath, index_col='mean_precisions')
                data["mean_time"].sort_index().plot(label=engine)

            plt.title(f"Search Latency - {dataset} - {parallel} threads")
            plt.xlabel("Precision")
            plt.ylabel("Latency")
            plt.grid(visible=True, which='major', axis='both', color='#F5F5F5', linestyle='--')
            plt.ylim(bottom=0)
            plt.legend()

            chart_path = DATA_DIR / f"data-latency-{dataset}-{parallel}.png"
            fig.savefig(chart_path)
            print("Chart generated: " + str(chart_path))


def gen_charts_upload_results(files):

    plt.style.use('default')

    for dataset, dataset_data in files.items():
        for engine, filepath in dataset_data.items():
            #fig, ax = plt.subplots(figsize=(16, 8), dpi=150)
            fig, ax = plt.subplots(figsize=(10, 5), dpi=150)
            ax.set_ylabel('Duration (s)')
            ax.set_title(f"Data Ingestion - {dataset} - {engine}")

            data = pd.read_csv(filepath)
            data.sort_values(by=['m', 'ef'], inplace=True, ascending = [True, True])
            upload_times = [data['upload_time'][i] for i in data.index]
            index_times = [data['total_time'][i] - data['upload_time'][i] for i in data.index]
            m_ef = ['m=%s,ef=%s' % (data['m'][i], data['ef'][i]) for i in data.index]
            p = ax.bar(m_ef, upload_times)
            p = ax.bar(m_ef, index_times, bottom=upload_times)
            ax.legend(labels=['Upload time', 'Index time'])
            for p in ax.patches:
                width, height = p.get_width(), p.get_height()
                x, y = p.get_xy()
                if int(height) == 0:
                    continue
                ax.text(x+width/2,
                    y+height/2,
                    '{:.0f} s'.format(height),
                    horizontalalignment='center',
                    verticalalignment='center')
            chart_path = DATA_DIR / f"data-upload-{engine}-{dataset}.png"
            fig.savefig(chart_path)
            print("Chart generated: " + str(chart_path))


def main():
    (upload_results, search_results) = load_results()
    search_files = save_search_results(search_results)
    upload_files = save_upload_results(upload_results)
    gen_charts_search_results(search_files)
    gen_charts_upload_results(upload_files)

if __name__ == '__main__':
    main()
