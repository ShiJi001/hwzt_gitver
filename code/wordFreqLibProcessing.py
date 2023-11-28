import os
import CoGenConfig as myconfig
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import cProfile
import json
from pypinyin import lazy_pinyin

def merge_and_sort_txt_files(file1_path, file2_path, output_file_path):
    """
    合并两个文本文件中的词语，并按字典顺序排序，然后输出到一个新的文件中。
    每个文件中的每一行应该包含一个词语。

    :param file1_path: 第一个文本文件的路径。
    :param file2_path: 第二个文本文件的路径。
    :param output_file_path: 输出文件的路径。
    """
    # 检查输入文件是否存在
    if not os.path.exists(file1_path):
        raise FileNotFoundError(f"文件不存在: {file1_path}")
    if not os.path.exists(file2_path):
        raise FileNotFoundError(f"文件不存在: {file2_path}")

    # 检查输出文件是否已存在
    if os.path.exists(output_file_path):
        print(f"警告: 输出文件 {output_file_path} 已存在，将被覆盖。")

    # 创建一个空集合用来存储词语
    words = set()

    # 读取文件并添加到集合
    for file_path in [file1_path, file2_path]:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                words.add(line.strip())

    # 对集合中的词语进行排序
    sorted_words = sorted(words)

    # 将排序后的词语写入到新的文件
    with open(output_file_path, "w", encoding="utf-8") as output_file:
        for word in sorted_words:
            output_file.write(word + "\n")


def is_all_chinese(s):
    """检查字符串是否全部由中文字符组成"""
    return all("\u4e00" <= char <= "\u9fff" for char in s)


def filter_keywords(csv_path, txt_path, output_path, n_values, blacklist):
    '''
    
    '''
    # 读取CSV文件
    df = pd.read_csv(csv_path)

    # 删除黑名单中的词性
    df = df[~df["词性"].isin(blacklist)]

    # 删除非中文关键词
    df = df[df["关键词"].apply(is_all_chinese)] 

    # 合并关键词并计算词频
    df = df.groupby("关键词")["词频"].sum().reset_index()

    # 读取词语库
    with open(txt_path, "r", encoding="utf-8") as file:
        words_set = set(line.strip() for line in file)

    for n in n_values:
        # 根据阈值n过滤数据
        filtered_df = df[df["词频"] >= n]

        # 取出不在词语库中的关键词
        final_words = filtered_df[~filtered_df["关键词"].isin(words_set)]["关键词"]

        final_words=sorted(final_words)

        # 输出结果到txt文件
        with open(
            os.path.join(output_path, f"keywords_result_threshold_{n}.txt"),
            "w",
            encoding="utf-8",
        ) as file:
            for word in final_words:
                file.write(f"{word}\n")


def filter_keywords_include_origin(csv_path, txt_path, output_path, n_values, blacklist):
    # 读取CSV文件
    df = pd.read_csv(csv_path)

    # 删除黑名单中的词性
    df = df[~df["词性"].isin(blacklist)]

    # 删除非中文关键词
    df = df[df["关键词"].apply(is_all_chinese)] 

    # 合并关键词并计算词频
    df = df.groupby("关键词")["词频"].sum().reset_index()

    # 读取词语库
    with open(txt_path, "r", encoding="utf-8") as file:
        words_from_txt = set(line.strip() for line in file)

    for n in n_values:
        # 根据阈值n过滤数据
        filtered_df = df[df["词频"] >= n]

        # 获取满足阈值的关键词
        words_meeting_threshold = set(filtered_df["关键词"])

        # 合并满足阈值的关键词和txt中的词语，并去重
        final_words = sorted(words_meeting_threshold.union(words_from_txt), key=lambda x: lazy_pinyin(x))

        # 输出结果到txt文件
        output_file_path = os.path.join(output_path, f"combined_keywords_threshold_{n}.txt")
        with open(output_file_path, "w", encoding="utf-8") as file:
            for word in final_words:
                file.write(f"{word}\n")


def filter_keywords_test(csv_path, txt_path, output_path, n_values, blacklist):
    '''
    输出文件拼音排序，并且输出被删除的部分。 
    '''
    # 读取CSV文件
    df = pd.read_csv(csv_path)

    # 保存原始DataFrame为original_df
    original_df = df.copy()

    # 删除黑名单中的词性
    df = df[~df["词性"].isin(blacklist)]

    # 删除非中文关键词
    df = df[df["关键词"].apply(is_all_chinese)] 

    # 合并关键词并计算词频
    df = df.groupby("关键词")["词频"].sum().reset_index()

    # 读取词语库
    with open(txt_path, "r", encoding="utf-8") as file:
        words_set = set(line.strip() for line in file)

    for n in n_values: 
        # 根据阈值n过滤数据
        filtered_df = df[df["词频"] >= n]

        # 取出不在词语库中的关键词
        final_df = filtered_df[~filtered_df["关键词"].isin(words_set)].copy()

        # 添加一个拼音列用于排序
        final_df['拼音'] = final_df['关键词'].apply(lambda x: ''.join(lazy_pinyin(x)))

        # 按照中文拼音排序
        final_df = final_df.sort_values(by="拼音")

        # 输出结果到txt文件
        with open(
            os.path.join(output_path, f"keywords_result_threshold_{n}.txt"),
            "w",
            encoding="utf-8",
        ) as file:
            for _, row in final_df.iterrows():
                file.write(f"{row['关键词']}:{row['词频']}\n")

        # 找出被过滤掉的词
        filtered_out_df = original_df[~original_df["关键词"].isin(filtered_df["关键词"])]

        filtered_out_df = filtered_out_df[filtered_out_df["词频"] >= n].copy()

        # 添加一个拼音列用于排序
        filtered_out_df['拼音'] = filtered_out_df['关键词'].apply(lambda x: ''.join(lazy_pinyin(x)))

        # 按照中文拼音排序
        filtered_out_df = filtered_out_df.sort_values(by="拼音")

        # 输出被过滤掉的词到txt文件
        filtered_out_file_path = os.path.join(output_path, f"filtered_out_words_threshold_{n}.txt")
        with open(filtered_out_file_path, "w", encoding="utf-8") as file:
            for _, row in filtered_out_df.iterrows():
                file.write(f"{row['关键词']}:{row['词频']}\n")



def main():
    csv_path = myconfig.CSV_path_WFLibProcessing
    txt_path = myconfig.txt_path_WFLibProcessing
    output_path = myconfig.resrootname_WFLibProcessing
    n_values = myconfig.Freq_n_WFLibProcessing
    blacklist = myconfig.WFLib_exclude_pos_WFLibProcessing

    profiler = cProfile.Profile()
    profiler.enable()

    # print("")
    print(f"{csv_path}开始处理。已有关键词库为：{txt_path},输出地址为：{output_path}。")
    print(f"当前阈值为：{n_values}，当前词性黑名单为：{blacklist}。")
    filter_keywords_include_origin(csv_path, txt_path, output_path, n_values, blacklist)

    # 时间戳
    timestamp = datetime.now().strftime("%m%d%H%M")
    profiler.disable()
    #是否保存性能分析结果
    saveTag=myconfig.save_profiler
    if(saveTag):
        profiler.dump_stats(
            f"performance_analysis_4wordsLibProcessing_{timestamp}.prof"
        )


if __name__ == "__main__":
    main()
