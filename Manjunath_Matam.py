# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 22:12:00 2021

Motivation: LBNL Programming assessment received from Dr. Anubhav Jain
@author: Manjunath Matam (manjunath.matam@ucf.edu, manjunath031@gmail.com)

Objective: Take two files (files.txt and nodes.txt), process them, 
          distribute the files to appropriate nodes, and produce an output file (result.txt). 

Two input files adhere to a common rules unless otherwise mentioned, are: 
    1. Blank lines or lines starting with # character can be ignored in the process. 
    2. The first column is a string, contains file/node name.
    3. The second column is an interger, shows the file size or node space in bytes. 

Distribution rules (Files to nodes) specified in the document are:
    1. 

Additional rules (not directly specifed in the document) considered are: 
    1. One node can accommodate two or more files.
    2. One file cannot be transferred to the multiple nodes. 
"""

#### Import few standard modules
import pandas as pd
import numpy as np

import argparse
import sys


def convert_txt2df(ip):
    df = []
    try:
        df = pd.read_csv('%s'%(ip), sep=" ", header=None)
        # print(df.head(n=5))
    except:
        print("Certain lines in the %s file have additional spaces, does not follow the standard format"%(ip))
    return df

def df_processing(df):
    """
    This function takes raw df as input and processes it, drops the lines 
    starting with '#', white spaces, and returns processed df. 
    """
    ### Special characters defined in the document
    spec_char = ["","#"]
    ### Replace special characters with NaN
    df = df.replace(spec_char,np.nan)
    ### Includ first two columns (as per the document) and drop rest if any present 
    df = df.loc[:,df.columns[:2]]
    ### Drop a row if NaN is present in any one column
    df.dropna(how='any',inplace=True)
    ### Reset the index, drop the old index
    df.reset_index(inplace=True,drop=True)
    ### convert columns to propoer format: first to string, second to integer 
    df[df.columns[0]] = df[df.columns[0]].astype('str') 
    df[df.columns[1]] = df[df.columns[1]].astype(int) 
    return df

if __name__ == '__main__':
    ### Directly import the files while working here for debugging
    # fdf = convert_txt2df('files.txt')
    # ndf = convert_txt2df('nodes.txt')
    ### command line interface codes for importing input files
    parser = argparse.ArgumentParser(description='\
            ### Description: The program takes two input files (files.txt, nodes.txt) and \
                             produces an output file (result.txt) in three steps')
    parser.add_argument('-f',"-input-files",type=str,dest="ip_file",required=True,choices=['files.txt'],
                        help='Input the correct file named files.txt')
    parser.add_argument('-n',"-input-nodes",type=str,dest='ip_node',required=True,choices=['nodes.txt'],
                        help = 'Input the correct file named nodes.txt')
    parser.add_argument('-o', type =str, default ='result.txt',required=False,dest='op_result',
                        help = 'Provide a name to the output file (Default: result.txt)')
    args = parser.parse_args()
    ### Print the namesapce object
    print(args)
    ### convert the input file to dataframe
    print("\n ### Converting files.txt to dataframe \n")
    fdf = convert_txt2df(args.ip_file)
    print("\n ### Converting files.txt to dataframe \n")
    ndf = convert_txt2df(args.ip_node)
    ### process the dataframe and drop the uncessary columns, characters
    print("\n Dropping blank lines, lines commented using (#) character ")    
    fdf = df_processing(fdf)
    ndf = df_processing(ndf)
    ### Name the columns; it is easier to process the named columns
    fdf.columns = ['filename','size']
    ndf.columns = ['nodename','space']
    print("\n ### Files-dataframe first 5 rows \n")
    print(fdf.head(n=5))
    print("\n ### Nodes-dataframe first 5 rows \n")
    print(ndf.head(n=5))
    
    ### Process the files and nodes to produce the output file
    ### compute total file size and node space 
    tfs = fdf['size'].sum(axis=0)
    tns = ndf['space'].sum(axis=0)
    ### Compare the data size with respect to available memory in nodes 
    dif = tfs*100/tns
    ### comparison
    comp = np.where(dif<=100,'can','cannot')
    print("\n Compared to the available memory (100%%), the data size is %d %%."%(dif))
    print("Hence, all the data %s be stored in the nodes. \n"%(comp))
    ### Allocation principles:
    ### 1. When data size is bigger than available spce, start dropping the bigger data files and until the remaining all can be accommodated in the nodes
    ### 2. When data is smaller than available space, accommodate equal size data into all the nodes
    fdf.sort_values('size',inplace=True,ignore_index=True,ascending=False)
    fdf['target_node'] = np.nan
    ndf.sort_values('space',inplace=True,ignore_index=True,ascending=False)
    ### Calculate maximum node space - nmax
    ### namx puts limit on the maximum file size that can transferred to the node
    nmax = ndf['space'].max(axis=0)
    ### identify the oversized sizes that cannot be accomodated in any nodes
    ### check if each file (row) can be transfered to the nodes 
    fdf1 = fdf[(fdf['size'] < tns) & (fdf['size'] < nmax) ]
    if len(fdf1) > 0:
        ### Reset the files df index
        fdf1.reset_index(inplace=True,drop=True)
        ### calculate min file size that will be transported
        tmin = int(fdf1['size'].min(axis=0))
        ### check if each node has enough space to accommodate the prescribed transfer limit
        ndf1 = ndf[ndf['space']>tmin]
        if len(ndf1)>0:
            ### calculate avg file size that can be transported to each node 
            tavg = int(fdf1['size'].sum(axis=0)/len(ndf1))
            
            ### 
            nf = len(fdf1)
            nn = len(ndf1)
            
           
            lst = list(range(0, nf, 1))
            flist = [lst[i:i + nn:1] for i in range(0, nf, nn)]
            
            # build new list where the sub-list are reversed if in odd indices
            revflist = [lst[::-1] if i % 2 == 1 else lst for i, lst in enumerate(flist)]
            
            # zip using zip_longest and filter out the None values (the default fill value of zip_longest)
            from itertools import zip_longest
        
            solflist = [[v for v in vs if v is not None] for vs in zip_longest(*revflist)]
            # print(solflist)
            
            ### Allocate the files in the order of nodes
            for i in range(len(solflist)):
                fdf1.at[solflist[i],'target_node'] = ndf1['nodename'].loc[i]
            ### Map the target_node to main files df
            result = fdf.set_index("filename").combine_first(fdf1.set_index("filename")).reset_index()
            result.replace(np.nan,'NULL',inplace=True)
        
            ### save the result
            output = result[['filename','target_node']]
            # result.to_csv('results.txt',header=False,index=False,sep=" ")    
            output.to_csv('%s'%(args.op_result),header=False,index=False, sep=" ")
            ### cross check the file size accommodated to each node
            chk = result.groupby(by='target_node').sum()
            chk = pd.merge(chk,ndf.set_index('nodename'),how='left',left_index=True,right_index=True)
            chk['Transferrable?'] = chk['size'] < chk['space']
            chk.rename(inplace=True, columns={"size":"File Size",
                                              "space":"Node Space"})
            print(chk.head())
        else:
            print("Exiting with no output file. Reason: none of the nodes have enough space to accommodate any of the files.")
    else:
        print("Exiting with no output file. Reason: none of the nodes have enough space to accommodate any of the files.")
    
    
   
