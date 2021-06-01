import itertools
from random import *
import csv

if __name__ == "__main__":
    a_list = list(range(1, 122))
    lst = list(itertools.permutations(a_list, 2))
    for i in range(0,len(lst)):
        f = (str(lst[i])+"'"+str(round(random(),8))+"'")
        f1 = f.replace("(", "")
        f2 = f1.replace(")", ", ")
        print(f2)
       
    
