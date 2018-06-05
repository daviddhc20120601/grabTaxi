from conf.settings import *
from producers import producer_order
from processors import consumer_order, ratio_cal
import sys


def main():
    para = sys.argv
    for i in para[1:]:
        print(i)
    if para[1] == "produce":
        producer_order.main(para[2])
    elif para[1] == "consume":
        consumer_order.main(para[2])
    elif para[1] == "calRatio":
        ratio_cal.main()


if __name__ == '__main__':
    print(">>>how to use>>> \n1 produce order \n2 produce vendor \n 3 consume order \n 4 consume vendor \n 5 calRatio \n 6 predictCong \n")
    main()
