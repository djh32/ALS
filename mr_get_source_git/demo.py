#coding=utf-8
import sys

def main():
    mid_dic = {}
    with open("./mid_list_last", 'r') as f:
        for l in f:
            lis = l.strip().split("\t")
            mid_real = lis[0]
            mid_id = lis[1].split("_")[0]
            mid_dic[mid_id] = mid_real

    with open(sys.argv[1], 'r') as f:
        for l in f:
            print mid_dic[l.strip()]

if __name__ == "__main__":
    main()







