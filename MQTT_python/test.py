import time


def mathematical_calculation():
    # Initialize denominator
    k = 1
    # Initialize sum
    s = 0

    for i in range(1000000):
        # even index elements are positive
        if i % 2 == 0:
            s += 4 / k
        else:
            # odd index elements are negative
            s -= 4 / k

        # denominator is odd
        k += 2
    return s
    # print(s)


if __name__ == "__main__":
    for i in range(2500):
        start_time = time.time()
        mathematical_calculation()
        end_time = time.time()
        print("TIME TAKEN BY MATHEMATICAL CALCULATION: " + str(end_time - start_time))