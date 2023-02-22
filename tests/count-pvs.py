import json
import pprint

count1 = {}
count2 = {}
count3 = {}

with open('PBI-archiver-01.json') as fp:
    data = json.load(fp)

    for item in data:
        parts = item.split(':')
        if parts[0] in count1:
            count1[parts[0]] += 1
        else:
            count1[parts[0]] = 1
        if parts[1] in count2:
            count2[parts[1]] += 1
        else:
            count2[parts[1]] = 1

        part12 = parts[0]+':'+parts[1]
        if part12 in count3:
            count3[part12] += 1
        else:
            count3[part12] = 1

print('\ncount1\n')
pprint.pprint(count1)
print('\ncount2\n')
pprint.pprint(count2)
print('\ncount3\n')
pprint.pprint(count3)
