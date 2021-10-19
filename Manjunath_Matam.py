# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 22:12:00 2021

Motivation: LBNL Programming assessment received from Dr. Anubhav Jain.
@author: Dr. Manjunath Matam (manjunath.matam@ucf.edu, manjunath031@gmail.com)

Objective: Take two files (files.txt and nodes.txt) through command line, process them, 
          distribute the files to appropriate nodes, and produce an output file (result.txt). 

Distribution principle: The author has implemented the Min-Max distribution plan to produce 
                        a solution to balance the data stored on the nodes, not the space on the nodes. 

Two input files that adhere to the specifications mentioned in the document are: 
    1. Blank lines or lines starting with # character can be ignored in the process. 
    2. The first column is a string, contains the file/node name.
    3. The second column is an integer, shows the file size or node space in bytes. 

Distribution rules (Files to nodes) specified in the document are:
    1. A file that cannot be accommodated in any node is denoted as NULL.

Additional rules (not directly specified in the document but) considered by the author are: 
    1. One node can accommodate two or more files.
    2. One file cannot be transferred to multiple nodes. 
    3. All the files with sizes greater than the maximum node are not stored, denoted as NULL.
    4. Duplicated entries will raise an error.  

"""

#### Import few standard modules
import pandas as pd
import numpy as np
import argparse
import sys
from itertools import zip_longest


## command line interface codes for importing input files
parser = argparse.ArgumentParser(
    usage="The program takes two input files (files.txt, nodes.txt) and produces an output file (result.txt) in three steps.",
    description="Description: The program uses Min-Max distribution plan for allocating files to nodes.",
)
parser.add_argument(
    "-f",
    "-input-files",
    type=str,
    dest="ip_file",
    required=True,
    choices=["files.txt"],
    help="Input the correct file named files.txt",
)
parser.add_argument(
    "-n",
    "-input-nodes",
    type=str,
    dest="ip_node",
    required=True,
    choices=["nodes.txt"],
    help="Input the correct file named nodes.txt",
)
parser.add_argument(
    "-o",
    type=str,
    default="result.txt",
    required=False,
    dest="op_result",
    help="Provide a name to the output file (Default: result.txt)",
)
args = parser.parse_args()
## Print the namesapce object
print(args)


def convert_txt2df(ip):
    """
    Parameters
    ----------
    ip : .txt file
        Contains two columns separated by white space and a few additional lines.
    Returns
    -------
    df : dataframe
        Contains the columns converted from the .txt file.
    Error: Text message
          Produces error when the .txt file contains non specified characters or additional spaces
    """
    df = []
    try:
        df = pd.read_csv("%s" % (ip), sep=" ", header=None)
        # print(df.head(n=5))
    except:
        print(
            "############## Certain lines in the %s file have additional spaces, does not follow the standard format"
            % (ip)
        )
    return df


def df_processing(df):
    """
    This function takes raw df as input and processes it, drops the lines
    starting with '#', white spaces, and returns processed df.

    Parameters
    ----------
    df : dataframe
        Contains un processed raw data.

    Returns
    -------
    df : dataframe
        Contains processed data after dropping blank or commented lines or rows with NaN values.
    """
    ### Special characters defined in the document
    spec_char = ["", "#"]
    ### Replace special characters with NaN
    df = df.replace(spec_char, np.nan)
    ### Includ first two columns (as per the document) and drop rest if any present
    df = df.loc[:, df.columns[:2]]
    ### Drop a row if NaN is present in any one column
    df.dropna(how="any", inplace=True)
    ### Reset the index, drop the old index
    df.reset_index(inplace=True, drop=True)
    ### convert columns to propoer format: first to string, second to integer
    df[df.columns[0]] = df[df.columns[0]].astype("str")
    df[df.columns[1]] = df[df.columns[1]].astype(int)
    ### Stop with error if there are duplicat entries in the names column
    if df[df.columns[0]].duplicated().any():
        print(
            "############### Duplicates have been found in the dataframe, hence exiting."
        )
        sys.exit()
    return df


### Main program begins
if __name__ == "__main__":
    ## convert the input file to dataframe
    print("\n######################################################")
    print("####### STEP1/3: Converting .txt document to dataframe:")
    print("############### Converting 'files.txt' to dataframe.")
    fdf = convert_txt2df(args.ip_file)
    print("############### Converting 'nodes.txt' to dataframe.")
    ndf = convert_txt2df(args.ip_node)

    ### process the dataframe and drop the uncessary columns, characters
    print("\n######################################################")
    print("####### STEP2/3: Processing the dataframes:")
    print(
        "############### Dropping blank lines, lines beginning with (#) character in files df."
    )
    fdf = df_processing(fdf)
    print(
        "############### Dropping blank lines, lines beginning with (#) character in nodes df."
    )
    ndf = df_processing(ndf)
    ### Name the columns; it is easier to process the named columns
    fdf.columns = ["filename", "size"]
    ndf.columns = ["nodename", "space"]

    ### Process the files and nodes to produce the output file
    print("\n######################################################")
    print(
        "####### STEP3/3: Implementing the Min-Max distribution plan for balancing the data stored on the nodes:"
    )
    ###tfs, tns: compute total size of the files and nodes
    tfs = fdf["size"].sum(axis=0)
    tns = ndf["space"].sum(axis=0)
    ### Compare the data size with respect to available memory in nodes
    dif = tfs * 100 / tns
    ### Compare whether all the data on files can/can't be stored on to the nodes
    comp = np.where(dif <= 100, "can", "cannot")
    print(
        "############## Compared to the available memory on all nodes (100%%), data size on the files is %d %%."
        % (dif)
    )
    print("############## Hence, all the data %s be stored in the nodes." % (comp))
    ### Distribution Allocation principles:
    ### 1. When data size is bigger than available spce, start dropping the bigger data files and until the remaining all can be accommodated in the nodes
    ### 2. When data is smaller than available space, accommodate equal size data into all the nodes using Min-Max (M2) algorithm
    ### 3. Proposed M2 algorithm reports 100% distribution accuracy when all the nodes have more space than average file size (sizes of all files/number of files)
    fdf.sort_values("size", inplace=True, ignore_index=True, ascending=False)
    fdf["target_node"] = np.nan
    ndf.sort_values("space", inplace=True, ignore_index=True, ascending=False)
    ### Calculate maximum node space: nmax
    ### namx puts limit on the maximum file size that can transferred to the node
    nmax = ndf["space"].max(axis=0)
    ### Files eligibility test: identify the oversized sizes that cannot be accomodated in any nodes
    ### check if each file (row) can be transfered to the nodes
    fdf1 = fdf[(fdf["size"] < tns) & (fdf["size"] < nmax)].copy()
    if len(fdf1) > 0:
        ### Reset the files df index
        fdf1.reset_index(inplace=True, drop=True)
        ### tmin: calculate min file size that will be transported
        tmin = int(fdf1["size"].min(axis=0))
        ### Nodes eligibility test: check if each node has enough space to accommodate the prescribed transfer limit
        ndf1 = ndf[ndf["space"] > tmin]
        if len(ndf1) > 0:
            print(
                "############## Oversized files or undersized nodes have been ignored."
            )
            ### tavg: calculate avg file size that can be transported to each node
            tavg = int(fdf1["size"].sum(axis=0) / len(ndf1))
            ### nf, nn: number of files and nodes that passed the eligibility test
            nf = len(fdf1)
            nn = len(ndf1)
            ### fidx: files index, 0,1,2,..,nf; 0 denotes big size file,..,nf denotes small size file
            fidx = list(range(0, nf, 1))
            flist = [fidx[i : i + nn : 1] for i in range(0, nf, nn)]
            ### reflist: new list where every odd list elements are reversed
            revflist = [lst[::-1] if i % 2 == 1 else lst for i, lst in enumerate(flist)]
            ### solflist: Group the first elements in all sub lists in revflist by their position
            solflist = [
                [v for v in vs if v is not None] for vs in zip_longest(*revflist)
            ]

            ### Allocate the files in the order of nodes
            for i in range(len(solflist)):
                fdf1.at[solflist[i], "target_node"] = ndf1["nodename"].loc[i]
            ### Map the target_node to main files df
            result = (
                fdf.set_index("filename")
                .combine_first(fdf1.set_index("filename"))
                .reset_index()
            )
            ### Replace NaN with NULL
            result.replace(np.nan, "NULL", inplace=True)

            ### output: save the result; drop header while saving
            output = result[["filename", "target_node"]]
            output.to_csv("%s" % (args.op_result), header=False, index=False, sep=" ")
            print("\n######################################################")
            print(
                "############## Distribution plan has been generated and saved as %s in the same folder."
                % (args.op_result)
            )
            ### chk: cross check each node's space > size of the file allocated to it and verify if it is a perfect distribution
            chk = result.groupby(by="target_node").sum()
            chk = pd.merge(
                chk,
                ndf.set_index("nodename"),
                how="left",
                left_index=True,
                right_index=True,
            )
            ### Transferable?: True denotes the node has space to accommodate the allocated file(s)
            chk.loc[:, "Transferrable?"] = chk["size"] < chk["space"]
            chk.rename(
                inplace=True, columns={"size": "File Size", "space": "Node Space"}
            )
            ### Distribution accuracy: how many of allocated nodes actually have space to accommodate the data
            ### True positives: node space > size of file allocated to it
            tp = len(chk[(chk["Transferrable?"]) & ((chk["Node Space"] != np.nan))])
            ### False positives: node space < size of file allocated to it
            fp = len(chk[(~chk["Transferrable?"]) & ((chk["Node Space"] == np.nan))])
            ### Distribution accuracy:
            da = tp * 100 / (tp + fp)
            print(
                "############## Also, the plan has been cross verified with %d%% accuracy."
                % (da)
            )
        else:
            print("\n######################################################")
            print(
                "############ Exiting with no output file. Reason: none of the nodes have enough space \
                  to accommodate any of the files."
            )
    else:
        print("\n######################################################")
        print(
            "############ Exiting with no output file. Reason: none of the nodes have enough space \
              to accommodate any of the files."
        )

### End of the code
