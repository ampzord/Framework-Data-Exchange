import time


# first solution will be starting with lower
def clientNumberLowestDivisor(clients):  # number of clients 5,10,15,20,25 ?
    lower_division = 2
    while not clients % lower_division == 0:
        lower_division += 1

    concurrent_clients = int(clients / lower_division) # 15
    clients_per_grp = lower_division # 2
    solution_string = []
    temp_sol = []

    # print("Concurrent clients:", concurrent_clients)
    # print("Clients per grp:", clients_per_grp)

    number_start_original = 1
    for i in range(clients_per_grp):
        number_start = number_start_original
        temp_sol = []
        for j in range(concurrent_clients):
            # print("number start:", number_start)
            temp_sol.append(str(number_start) + ",")
            number_start += clients_per_grp

        solution_string.append(temp_sol)
        number_start_original += 1
        # print("Number start original:", number_start_original)

    # print(solution_string)
    return solution_string, concurrent_clients, clients_per_grp

def information_requested(decoded_message):
    if "GET_INFORMATION_" in decoded_message:
        # parse decoded message
        split_string = decoded_message.split("_")  # 1,4,7,10,13,
        print("Split String :", split_string)
        split_of_comma_string = split_string.split(",")  # 1 4 7 10 13
        for i in range(len(split_of_comma_string)):
            if "1" in split_of_comma_string[i]:
                return True
    return False



if __name__ == "__main__":
    information_requested("GET_INFORMATION_FROM_client1")
