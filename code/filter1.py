import pandas as pd
import os

#读取txt文件，筛选只在txt文件中存在的数据行
def filter_pairs(csv_file_path, txt_file_path):
    # 读取txt文件并将每一对词语存储在一个集合中
    pairs = set()
    with open(txt_file_path, 'r', encoding='utf-8') as txt_file:
        for line in txt_file:
            word1, word2 = line.strip().split('，')  # 注意这里使用的是中文逗号
            pairs.add((word1, word2))
    
    # 读取csv文件
    df = pd.read_csv(csv_file_path)
    
    # 筛选出只在pairs集合中存在的数据行
    filtered_df = df[df.apply(lambda row: (row['词语1'], row['词语2']) in pairs, axis=1)]
    
    return filtered_df


#过滤关键词库中词频大于等于n的条目并且存储为txt
def filter_keywords(csv_path, n, txt_output_path):

    data = pd.read_csv(csv_path)
    
    filtered_data = data[data['词频'] >= n]

    filtered_data.to_csv(txt_output_path, sep='\t', index=False, header=True)



def main():

    # 使用该函数
    csv_file_path = r'/home/xt/workplace/hwzt/res/keywords/wordsLibrary_94w.csv'  # csv文件的路径
    txt_file_path = r'/home/xt/workplace/hwzt/res/keywords/wordsLibrary_94w.txt'  # txt文件的路径

    #filter_keywords(csv_file_path,3,txt_file_path)

    
    filtered_data = filter_pairs(csv_file_path, txt_file_path)
    #存储结果为csv文件
    filtered_data.to_csv( r'/home/xt/workplace/hwzt/res/keywords/wordsLibrary_94w.csv' , index=False)


if __name__ == "__main__":
    main()
