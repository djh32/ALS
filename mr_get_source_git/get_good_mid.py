#coding=utf-8
import sys
import math

def main():
    rtn_dic = {}
    with open(sys.argv[1],'r') as src_file:
        for line in src_file:
            (mid, zhuan, ping, zan, bofang) = line.split("\t")
            repost = float(zhuan) + 1
            comment = float(ping) + 1
            attitude = float(zan) + 1
            play = float(bofang) + 1
            try:
                #score = math.log(play,3) + math.log(repost,1.75) + math.log(comment,2.15) +math.log(attitude,2)
                #score = play * 0.2 + (comment + attitude + repost) *0.4
                #score = play*0.01 + comment + attitude + repost
                score = (math.log(comment,2) + math.log(repost,2) + math.log(attitude,2) ) * math.log(play,2)
            except Exception:
                #print line
                continue
            rtn_dic[mid] = {'score':score,'play':play}

    sort_list_remover_largeplay = sorted(rtn_dic.items(), key=lambda a:a[1]['play'], reverse=True)[200:300000]      #去除最热的200个mid热门播放
    sort_list = sorted(sort_list_remover_largeplay, key=lambda a:a[1]['score'], reverse=True)[:100000]      #按照action排10万个
    #sort_list_2 = sorted(sort_list, key=lambda a:a[1]['play'], reverse=True)[500:]           #再按照play 排去掉大热前1千个播放最多的
    f = open('./sort_result','w')
    for tup in sort_list:
        f.writelines(tup[0]+"\t"+"%.3f"%tup[1]['score']+"\n")

if __name__ == "__main__":
    main()
