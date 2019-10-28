##
# LDA Analysis Report
# 'back_to_review'
#
# GIDE INC 2019
##


from lda_analysis import topic_output, rev_data

# Bag of words and key map to the bag of words
# Main topics are for 0 to n topics outputting the actual terms
bag_of_words, key_map, main_topics = topic_output()


# dict_builder:
# Input: [List] bag_of_words
# Output: [Dict] dictionary

def dict_builder(bag_of_words):
    dictionary = {}
    counter = 0
    for element in bag_of_words:
        dictionary[element] = counter
        counter += 1
    return dictionary


# merge:
# Purpose: helper function that merges 2 sorted list of tuples comparing their field specified by index
# Input: [Dict] set1, [Dict] set2, [Int] index
# Output: [List] result

def merge(set1,set2,index):
    result = []
    temp1 = len(set1)
    temp2 = len(set2)
    i1 = 0
    i2 = 0
    while(i1<temp1 and i2<temp2):
        if (set1[i1])[index] >= (set2[i2])[index]:
            result.append(set1[i1])
            i1+=1
        else:
            result.append(set2[i2])
            i2+=1
    if i1>=temp1 and i2 >=temp2:
        pass
    elif i1<temp1:
        while i1<temp1:
            result.append(set1[i1])
            i1+=1
    else:
        while i2<temp2:
            result.append(set2[i2])
            i2+=1
    return result


# sort_tuple:
# Purpose: function that sort a tuple by its value at index
# Input: [List] set, [Int] index
# Output: None (sorted for 'merge' function)

def sort_tuple(set, index):
    temp = len(set)
    if temp <= 1:
        return set
    else:
        first_half = sort_tuple(set[0:temp//2],index)
        second_half = sort_tuple(set[temp//2:temp],index)
        return merge(first_half,second_half,index)


# reviewToKeyword:
# Purpose: Extract sentences and find them back to original keywords based on weight importance and if the sentence
# contains the keyword
# Input: [String] sentence
# Output: [List] temp (list of keywords)
def reviewToKeyword(sentence):

    dictionary = dict_builder(bag_of_words=bag_of_words)
    list_of_words = sentence.split()
    new_list_of_words = []
    temp = []
    for element in list_of_words:
        l = len(element)
        i = 0
        # check how many trailing punctuations there are in the words
        while i <= len(element):
            if element[l-1-i] == '.'or element[l-1-i] == ',' or element[l-1-i] == ';' or \
                    element[l-1-i] == '?' or element[l-1-i] == '!' or element[l-1-i] == ':':
                i += 1
            else:
                break
        # clear the punctuations at the end of the words
        if i > 0:
            new_list_of_words.append(element[:l-i])
        else:
            new_list_of_words.append(element[:l])
    # print(new_list_of_words)
    for element in new_list_of_words:
        # give sentinel value for i and use dictionary to find the value of word, stays -1 if not found
        i = -1
        try:
            i = dictionary[element]
        except KeyError:
            pass
        if i != -1:
            temp.append(element)
    return temp

# reviewToTopic:
# Purpose: Extract sentences and find them back to original keywords based on weight importance and if the sentence
# contains the keyword, and match back to the original topic
# Input: [String] sentence
# Output: [List] result (list of topics)
def reviewToTopic(sentence):

    dictionary = dict_builder(bag_of_words=bag_of_words)
    list_of_words = sentence.split()
    new_list_of_words = []
    list_topics = []
    for i in range(len(key_map)):
        list_topics.append(0)
    for element in list_of_words:
        l = len(element)
        i = 0
        # check how many trailing punctuations there are in the words
        while i <= len(element):
            if element[l - 1 - i] == '.' or element[l - 1 - i] == ',' or element[l - 1 - i] == ';' or \
                    element[l - 1 - i] == '?' or element[l - 1 - i] == '!' or element[l - 1 - i] == ':':
                i += 1
            else:
                break
        # clear the punctuations at the end of the words
        if i > 0:
            new_list_of_words.append(element[:l - i])
        else:
            new_list_of_words.append(element[:l])
    # print(new_list_of_words)
    for element in new_list_of_words:
        # give sentinel value for i and use dictionary to find the value of word, stays -1 if not found
        i = -1
        try:
            i = dictionary[element]
        except KeyError:
            pass
        if i != -1:
            count1 = 0
            # the following loop goes through the list of key_map and checks whether the the word in the corpus is in one of the topics
            while count1 < len(key_map):
                count2 = 0
                while count2 <len(key_map[count1][0]):
                    # if it is in one of the topics, add the weight of that word onto the corresponding topic
                    # print("{}|{}|{}".format(i,key_map[count1][0],count2))
                    if i == key_map[count1][0][count2]:
                        list_topics[count1] += key_map[count1][1][count2]
                    count2 +=1
                count1 +=1
    # find the max value in the topic weight list
    max_val = max(list_topics)
    temp = []
    result = []
    for i in range(len(list_topics)):
        # print("{}|{}".format(i,list_topics[i]))
        temp.append((i,list_topics[i]))
    sorted_list_topics = sort_tuple(temp, 1)
    # if max_val is 0, none of the topics are mentioned
    if max_val == 0:
        return result
    # if max_val is not 0, return the corresponding group that has the max_val
    else:
        for i in range(len(sorted_list_topics)):
            if sorted_list_topics[i][1] != 0:
                result.append(sorted_list_topics[i][0]+1)
        return result
