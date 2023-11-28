import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime
import cProfile
import json
import gc
import re
import CoGenConfig as myconfig

# 定义两种情况的属性列
full_columns = myconfig.words_full_columns
partial_columns = myconfig.words_partial_columns

#当前版本，修改了输入，由于运行较快，因此不再手动分块运行再合并，而是输入一个文件夹列表统一进行计算。并且增添了配置文件。

def rule_1(word):
    """
    规则1：只有长度为2-6的中文汉字构成的词才纳入统计。
    :param word: 待检查的词
    :return: bool, 该词是否满足规则1
    """
    if 2 <= len(word) <= 6 and re.match(r"^[\u4e00-\u9fa5]+$", word):
        return True
    return False


def singleSentenceProcess(sentTok, sentPos, need_pos):
    """处理单一句子，统计关键词及其词频"""
    keywords = {}
    for word, pos in zip(sentTok, sentPos):
        if pos in need_pos and rule_1(word):  # 仅统计需要的词性和满足rule_1的词
            key = (word, pos)
            keywords[key] = keywords.get(key, 0) + 1
    return keywords


def readArticle_fromJSP(article, need_pos):
    """从JSON文件读取文章，统计关键词及其词频"""
    tok_tasks = "tok/fine"
    pos_tasks = "pos/pku"

    articleKeywords = {}  # 初始化文章关键词词频字典

    aritcal_tok_list = article[tok_tasks]
    aritcal_pos_list = article[pos_tasks]

    for sentence_tok_temp, sentence_pos_temp in zip(aritcal_tok_list, aritcal_pos_list):
        singleSentenceKeywords = singleSentenceProcess(
            sentence_tok_temp, sentence_pos_temp, need_pos
        )
        for key, value in singleSentenceKeywords.items():
            articleKeywords[key] = articleKeywords.get(key, 0) + value

    data = [(word, pos, freq) for (word, pos), freq in articleKeywords.items()]
    return pd.DataFrame(data, columns=full_columns)


def mergeLibrary(df1, df2):
    """合并两个库"""

    if set(df1.columns) == set(full_columns) and set(df2.columns) == set(full_columns):
        merged_df = pd.concat([df1, df2])
        merged_df = merged_df.groupby(partial_columns, as_index=False).sum()
    else:
        raise ValueError("两个dataframe的属性列不符合要求")

    return merged_df



def corpus_process_and_merge(
    corpus_of_Json_folders, word_Freq_Library_folder, existed_wordFreqLibrary, need_pos
):
    """处理语料库并合并结果"""
    datacolumns = full_columns  # 这里假设 full_columns 已经被定义

    # 尝试读取已有的关键词库，仅在函数开始时执行一次
    wordFreqLibrary_to_be_added = pd.DataFrame(columns=datacolumns)  # 初始化新关键词库
    try:
        wordFreqLibrary_existed = pd.read_csv(existed_wordFreqLibrary)
        print(f"正在将已有关键词库{existed_wordFreqLibrary}读入内存。")
        if list(wordFreqLibrary_to_be_added.columns) == list(
            wordFreqLibrary_existed.columns
        ):
            wordFreqLibrary_to_be_added = wordFreqLibrary_existed
            wordFreqLibrary_existed = None
            gc.collect()
        else:
            print(f"输入的关键词库{existed_wordFreqLibrary}属性列不符合要求，属性列应为{datacolumns}。请检查。")
        print(f"已有关键词库{existed_wordFreqLibrary}读取完成。")
    except FileNotFoundError:
        print("不存在已有关键词库，正在新建关键词库。")
    except Exception as e:
        print(f"错误：读取已有关键词库{existed_wordFreqLibrary}时出现问题，问题如下：{str(e)}")

    # 检查输出文件夹是否存在，如果不存在则创建
    if not os.path.exists(word_Freq_Library_folder):
        os.makedirs(word_Freq_Library_folder)

    # 为每个JSON文件夹地址创建一个外部进度条
    pbar0 = tqdm(corpus_of_Json_folders, desc="总体进度")
    for corpus_of_Json_folder in pbar0:
        if not os.path.exists(corpus_of_Json_folder):
            print(f"错误：指定的文件夹 {corpus_of_Json_folder} 不存在。")
            continue

        # 获取corpus_of_Json_folder中的folder1列表
        folder1_list = os.listdir(corpus_of_Json_folder)
        folder1_list = sorted(folder1_list)  # 按照文件夹名字升序排序

        # 使用外部进度条处理folder1
        pbar1 = tqdm(folder1_list, desc=f"正在处理 {corpus_of_Json_folder} 中的文件夹", leave=False)
        for folder1 in pbar1:
            folder1_path = os.path.join(corpus_of_Json_folder, folder1)
            folder2_list = os.listdir(folder1_path)
            folder2_list = sorted(folder2_list)  # 按照文件夹名字升序排序

            # 遍历folder1中的folder2
            for folder2 in folder2_list:
                folder2_path = os.path.join(folder1_path, folder2)
                article_list = os.listdir(folder2_path)

                # 初始化一个空的dataframe来存放folder2的（词，词性）组
                folder2_Library_to_be_added = pd.DataFrame(columns=full_columns)

                # 遍历folder2中的每篇文章
                for article_file in article_list:
                    if not article_file.endswith(".json"):
                        print(f"警告：文件 '{article_file}' 不是json格式，已跳过。")
                        continue

                    file_path = os.path.join(folder2_path, article_file)

                    # 读取和处理文件
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            article_content = json.load(f)
                            # 假设 readArticle_fromJSP 和 mergeLibrary 已被定义
                            articleCollocation = readArticle_fromJSP(
                                article_content, need_pos
                            )
                            folder2_Library_to_be_added = mergeLibrary(
                                folder2_Library_to_be_added, articleCollocation
                            )
                    except MemoryError:
                        print(f"内存错误：处理文件 '{article_file}' 时内存不足。")
                    except Exception as e:
                        print(f"错误：处理文件 '{article_file}' 时出现问题。{str(e)}")

                # 合并folder1的结果
                try:
                    wordFreqLibrary_to_be_added = mergeLibrary(
                        wordFreqLibrary_to_be_added, folder2_Library_to_be_added
                    )
                except MemoryError:
                    print(f"内存错误：合并folder1 '{folder1}' 的（词，词性）组时内存不足。")
                    continue

            # 暂时保存当前结果
            wordFreqLibrary_new = wordFreqLibrary_to_be_added
            #print(f"开始保存截止至 '{folder1}'(包含) 的处理结果")
            try:
                timestamp = datetime.now().strftime("%m%d%H%M")
                wordFreqLibrary_new = wordFreqLibrary_new.sort_values(by="词频", ascending=False)
                wordFreqLibrary_new.to_csv(
                    os.path.join(
                        word_Freq_Library_folder,
                        f"wordFreqLibrary-temp-{timestamp}.csv",
                    ),
                    index=False,
                    encoding="utf-8-sig",
                )
                #print(f"截止 '{folder1}' 的处理结果已保存至wordFreqLibrary-temp-{timestamp}.csv")
            except Exception as e:
                print(f"错误：保存截止 '{folder1}' 的结果时出现问题。{str(e)}")
            wordFreqLibrary_new = None
            gc.collect()

    # 保存所有处理结果的代码
    print(f"开始保存所有的处理结果")
    wordFreqLibrary_new = wordFreqLibrary_to_be_added
    timestamp = datetime.now().strftime("%m%d%H%M")
    wordFreqLibrary_new = wordFreqLibrary_new.sort_values(by="词频", ascending=False)
    wordFreqLibrary_new.to_csv(
        os.path.join(word_Freq_Library_folder, f"wordFreqLibrary-{timestamp}.csv"),
        index=False,
        encoding="utf-8-sig",
    )



def main():
    rootname = myconfig.rootname_json2words
    #用于命名
    partname=myconfig.partname_json2words
    #实际地址
    partnames = myconfig.partnames_json2words
    resrootname = myconfig.resrootname_json2words

    #yuliaoku = os.path.join(rootname, partname)
    yuliaoku = [os.path.join(rootname, part) for part in partnames]
    dapeikujieguo = os.path.join(resrootname, partname)
    yiyoudapeiku = myconfig.existed_colLib_path_json2words

    # 纳入统计的词性列表
    # 需要学习的词性列表（切割规则细分）：
    # 【a, ad, an,d,i,j,l,n,ns,nz,v,vd,vn,z】

    # 需要学习的词性列表（切割规则粗分）：
    # 【a, ad, an,d,i,j,l,ns,nt,nz,v,vd,vn,z】

    # 细分词性列表
    need_pos = myconfig.wordsLib_need_pos_fine

    profiler = cProfile.Profile()
    profiler.enable()

    print("")
    print(f"{partnames}开始处理。已有关键词库为：[{yiyoudapeiku}]")
    corpus_process_and_merge(yuliaoku, dapeikujieguo, yiyoudapeiku, need_pos)

    # 时间戳
    timestamp = datetime.now().strftime("%m%d%H%M")
    profiler.disable()
    profiler.dump_stats(f"performance_analysis_4wordsLib_{partname}_{timestamp}.prof")


if __name__ == "__main__":
    main()
