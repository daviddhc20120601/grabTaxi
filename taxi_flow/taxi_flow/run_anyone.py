from conf.settings import *
from producers import producer_order
from processors import consumer_order, ratio_cal, congestions_cal
import sys
import argparse


def main():
    # para = sys.argv
    # for i in para[1:]:
    #     print(i)

    parser = argparse.ArgumentParser(description="this is a wrapper for grab taxi")
    parser.add_argument("script", action="store", help="producer , consumer , calRatio , calCong")
    parser.add_argument("order_or_vender", action="store", default=None, help="vendor or order")
    para = parser.parse_args()
    print(para)
    if para[1] == "produce":
        producer_order.main(para[2])
    elif para[1] == "consume":
        consumer_order.main(para[2])
    elif para[1] == "calRatio":
        ratio_cal.main()
    elif para[1] == "calCong":
        congestions_cal.main()

if __name__ == '__main__':
    # print(
    #     ">>>how to use>>> \n1 produce order \n2 produce vendor \n3 consume order \n4 consume vendor \n5 calRatio \n6 predictCong \n")
    main()

