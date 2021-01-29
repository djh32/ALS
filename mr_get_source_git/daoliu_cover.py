#coding=utf-8

def get_mid_set(src):
    rtn_set = set()
    with open(src,'r') as f:
        for l in f:
            mid = l.split("\t")[0]
            #if len(mid) > 11:continue
            rtn_set.add(mid.strip())
    return rtn_set

def main():
    s1 = get_mid_set("mid_list")
    s2 = get_mid_set("../material_mid")
    a = s1-s2
    for i in a:
        print i
    print   (s1 - s2)
    #print "jiaoji is " + str(len(s1 & s2))
    #print "tuijian is" +str(len(s2))
    #print "rate is "+str(len(s1 & s2) * 1.0 / len(s2))


if __name__ == "__main__":
    main()


