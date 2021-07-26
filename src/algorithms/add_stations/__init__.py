fact = [
60274032,
60280831,
60393808,
60418829,
60484672,
60532645,
60637527,
60733839,
60864709,
60867207,
60871316,
61066783,
61267134,
61361267,
61386538,
61500872,
62255385,
62289640,
62401963,
62504279,
63145536,
65280349,

]


report = [
60274032,
60280831,
60393808,
60418829,
60484672,
60532645,
60637527,
60733839,
60864709,
60867207,
60871316,
61267134,
61361267,
61386538,
61500872,
62255385,
62289640,
62401963,
62504279,
63145536,
65280349,

]



diff = []
for i in fact:
    if i not in report:
        print(i)
        diff.append(i)
        
diff_count = {}


def number_of_appearance(number, fact):
    count = 0

    for i in fact:
        if i == number:
            count += 1

    return count



for i in fact:
    diff_count[i] = number_of_appearance(i, fact)

print(diff_count)