import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime
import cProfile
import json
import gc
import CoGenConfig as myconfig

# 定义两种情况的属性列
full_columns = myconfig.full_columns
partial_columns = myconfig.partial_columns


def singleSentenceProcess(sentTok, sentPos, sentDep):
    '''
    对单个句子进行特定处理。
    '''
    # 创建一个新的dataframe
    singleSentenceCollocation = pd.DataFrame(columns=partial_columns)

    # 根据句法分析结果填充dataframe
    for i, (center_idx, relation) in enumerate(sentDep):
        # 获取中心词和当前词，并确保word1的索引小于word2
        if center_idx != 0 and center_idx - 1 <= i:
            word1 = sentTok[center_idx - 1]
            word2 = sentTok[i]
            pos1 = sentPos[center_idx - 1]
            pos2 = sentPos[i]
        elif center_idx != 0 and center_idx - 1 > i:
            word1 = sentTok[i]
            word2 = sentTok[center_idx - 1]
            pos1 = sentPos[i]
            pos2 = sentPos[center_idx - 1]
        else:
            continue  # 如果center_idx是0，即当前词是root，我们就跳过不添加

        new_row = pd.DataFrame(
            {
                "词语1": [word1],
                "词语2": [word2],
                "词语1词性": [pos1],
                "词语2词性": [pos2],
                "词语间依存关系": [relation],
            }
        )

        singleSentenceCollocation = pd.concat(
            [singleSentenceCollocation, new_row], ignore_index=True
        )

    # 规则处理
    singleSentenceCollocation = rule_1(singleSentenceCollocation)
    singleSentenceCollocation = rule_3(singleSentenceCollocation)
    singleSentenceCollocation = rule_2(singleSentenceCollocation, sentTok, sentDep)

    return singleSentenceCollocation


def rule_1(singleSentenceCollocation):
    '''
    规则1：去除标点符号和root的依存关系。
    '''
    biaodianTag = "punct"  # 标点符号 tag
    rootTag = "root"

    # 根据tag去除相应行数据
    singleSentenceCollocation = singleSentenceCollocation[
        (singleSentenceCollocation["词语间依存关系"] != biaodianTag)
        & (singleSentenceCollocation["词语间依存关系"] != rootTag)
    ]

    return singleSentenceCollocation


def rule_2(singleSentenceCollocation, sentTok, sentDep):
    '''
    规则2：对于复合名词，只保留离该组复合名词根节点最近的那一个搭配，注意，当前默认根节点是一组复合名词中最后一个名词。
    '''
    # 初始化一个空的DataFrame用于存储最终结果
    filtered_collocations = pd.DataFrame(columns=singleSentenceCollocation.columns)

    # 第一步：收集所有compound:nn依存关系的中心词
    center_words = {
        center_idx
        for _, (center_idx, relation) in enumerate(sentDep)
        if relation == "compound:nn"
    }

    # 第二步：对于每个中心词，找到最近的搭配
    for center_idx in center_words:
        nearest_collocation = None
        min_distance = float("inf")

        # 遍历所有搭配，寻找最近的搭配
        for i, row in singleSentenceCollocation.iterrows():
            if (
                row["词语间依存关系"] == "compound:nn"
                and row["词语2"] == sentTok[center_idx - 1]
            ):
                if row["词语1"] in sentTok:
                    current_distance = abs(center_idx - 1 - sentTok.index(row["词语1"]))
                    if current_distance < min_distance:
                        nearest_collocation = row
                        min_distance = current_distance

        # 添加最近的搭配到结果DataFrame中
        if nearest_collocation is not None:
            filtered_collocations = pd.concat(
                [filtered_collocations, nearest_collocation.to_frame().T],
                ignore_index=True,
            )

    # 移除原DataFrame中所有“compound:nn”依存关系的搭配
    singleSentenceCollocation = singleSentenceCollocation[
        singleSentenceCollocation["词语间依存关系"] != "compound:nn"
    ]

    # 合并处理后的搭配
    singleSentenceCollocation = pd.concat(
        [singleSentenceCollocation, filtered_collocations], ignore_index=True
    )

    return singleSentenceCollocation


# pku的代词可能有问题
def rule_3(singleSentenceCollocation):
    '''
    规则3：将人名与数词用占位符@和#替代。
    '''
    renming_cixing = "nr"  # 人名词性tag
    shuci_cixing = "m"  # 数词词性tag

    # 占位符标志
    renming_placeholder = "@"  # 人名
    shuci_placeholder = "#"  # 数词

    # 根据词性将单词替换为占位符
    singleSentenceCollocation.loc[
        singleSentenceCollocation["词语1词性"] == renming_cixing, "词语1"
    ] = renming_placeholder
    singleSentenceCollocation.loc[
        singleSentenceCollocation["词语1词性"] == shuci_cixing, "词语1"
    ] = shuci_placeholder

    singleSentenceCollocation.loc[
        singleSentenceCollocation["词语2词性"] == renming_cixing, "词语2"
    ] = renming_placeholder
    singleSentenceCollocation.loc[
        singleSentenceCollocation["词语2词性"] == shuci_cixing, "词语2"
    ] = shuci_placeholder

    return singleSentenceCollocation


def mergeLibrary(df1, df2):
    '''
    合并两个搭配库。
    '''
    # 情况1：一个dataframe的列是full_columns，另一个是partial_columns
    if (
        set(df1.columns) == set(full_columns)
        and set(df2.columns) == set(partial_columns)
    ) or (
        set(df1.columns) == set(partial_columns)
        and set(df2.columns) == set(full_columns)
    ):
        if set(df1.columns) == set(partial_columns):
            df1["搭配频次"] = 1
        else:
            df2["搭配频次"] = 1

        merged_df = pd.concat([df1, df2])
        merged_df = merged_df.groupby(partial_columns, as_index=False).sum()

    # 情况2：两个dataframe的列都是full_columns
    elif set(df1.columns) == set(full_columns) and set(df2.columns) == set(
        full_columns
    ):
        merged_df = pd.concat([df1, df2])
        merged_df = merged_df.groupby(partial_columns, as_index=False).sum()

    # 其他情况：报错并终止函数
    else:
        raise ValueError("两个dataframe的属性列不符合要求")

    return merged_df

def singleSentenceProcess_only_nn(sentTok, sentPos):
    """
    提取句子中所有的复合名词搭配。

    :param sentTok: 分词后的词语列表
    :param sentPos: 分词对应的词性列表
    :return: 复合名词搭配的表
    """

    # 名词词性集合，只有这些词性的词才被视为名词
    noun_pos_set = myconfig.noun_pos

    # 存储复合名词搭配的表
    # 创建一个新的dataframe
    singleSentenceCollocation = pd.DataFrame(columns=partial_columns)

    # 用于记录当前正在处理的复合名词
    current_compound_noun = []
    
    # 遍历句子中的每个词
    for tok, pos in zip(sentTok, sentPos):
        # 检查当前词是否为名词
        if pos in noun_pos_set:
            # 如果是名词，则添加到当前复合名词列表中
            current_compound_noun.append((tok, pos))
        else:
            # 如果不是名词，处理之前收集的复合名词
            if len(current_compound_noun) > 1:
                # 提取复合名词的搭配
                for i in range(len(current_compound_noun) - 1):
                    # 每两个相邻的名词组成一条搭配
                    word1, pos1 = current_compound_noun[i]
                    word2, pos2 = current_compound_noun[i + 1]
                    # 添加搭配到表中
                    new_row = pd.DataFrame(
                        {
                            "词语1": [word1],
                            "词语2": [word2],
                            "词语1词性": [pos1],
                            "词语2词性": [pos2],
                            "词语间依存关系": 'compound:nn',
                        }
                    )

                    singleSentenceCollocation = pd.concat(
                        [singleSentenceCollocation, new_row], ignore_index=True
                    )
            # 清空当前复合名词列表
            current_compound_noun = []

    # 处理句子末尾的复合名词（如果有）
    if len(current_compound_noun) > 1:
        for i in range(len(current_compound_noun) - 1):
            word1, pos1 = current_compound_noun[i]
            word2, pos2 = current_compound_noun[i + 1]
            new_row = pd.DataFrame(
                {
                    "词语1": [word1],
                    "词语2": [word2],
                    "词语1词性": [pos1],
                    "词语2词性": [pos2],
                    "词语间依存关系": 'compound:nn',
                }
            )

            singleSentenceCollocation = pd.concat(
                [singleSentenceCollocation, new_row], ignore_index=True
            )

    return singleSentenceCollocation


def readArticle_fromJSP(article):
    '''
    对一篇json格式存储的文章进行处理。
    '''
    tok_tasks = "tok/fine"
    pos_tasks = "pos/pku"
    dep_tasks = "dep"

    # 初始化两个空的dataframe来存放文章的搭配
    articleCollocation = pd.DataFrame(columns=partial_columns)

    # 从hanlp结果中获取三个列表
    aritcal_tok_list = article[tok_tasks]
    aritcal_pos_list = article[pos_tasks]
    aritcal_dep_list = article[dep_tasks]

    # 遍历SSentencList中的每个句子
    for sentence_tok_temp, sentence_pos_temp, sentence_dep_temp in zip(
        aritcal_tok_list, aritcal_pos_list, aritcal_dep_list
    ):
        #正常处理
        singleSentenceCollocation = singleSentenceProcess(
            sentence_tok_temp, sentence_pos_temp, sentence_dep_temp
        )
        #只要复合名词时
        # singleSentenceCollocation = singleSentenceProcess_only_nn(
        #     sentence_tok_temp, sentence_pos_temp
        # )
        # 将得到的dataframe数据添加到相应的文章dataframe中
        articleCollocation = pd.concat(
            [articleCollocation, singleSentenceCollocation], ignore_index=True
        )

    return articleCollocation


def corpus_process_and_merge(
    corpus_of_Json_folder, collocation_Library_folder, existed_coLibrary
):
    '''
    对多层文件夹的json形式文章进行处理，注意文件夹层级。
    '''
    datacolumns = full_columns

    # 检查corpus_of_Json_folder是否存在
    if not os.path.exists(corpus_of_Json_folder):
        print(f"错误：指定的文件夹{corpus_of_Json_folder}不存在。")
        return

    # 检查collocation_Library_folder是否存在，不存在则创建
    if not os.path.exists(collocation_Library_folder):
        os.makedirs(collocation_Library_folder)

    # 初始化一个空的dataframe来存放新的搭配
    CollocationLibrary_to_be_added = pd.DataFrame(columns=datacolumns)

    # 尝试读取已存在的库
    try:
        collocationLibrary_existed = pd.read_csv(existed_coLibrary)
        print(f"正在将已有搭配库{existed_coLibrary}读入内存。")

        # 检查是否具有相同的属性列。
        if list(CollocationLibrary_to_be_added.columns) == list(
            collocationLibrary_existed.columns
        ):
            # 将新的搭配与已存在的库合并
            CollocationLibrary_to_be_added = collocationLibrary_existed
            # 清空collocationLibrary_existed并进行垃圾回收
            collocationLibrary_existed = None
            gc.collect()

        else:
            print(f"输入的搭配库{existed_coLibrary}属性列不符合要求，属性列应为{datacolumns}。请检查。")

        print(f"已有搭配库{existed_coLibrary}读取完成。")

    except FileNotFoundError:
        # 如果已存在的库不存在，只使用新的搭配
        print("不存在已有搭配库，正在新建搭配库。")

    except Exception as e:
        print(f"错误：读取已有搭配库{existed_coLibrary}时出现问题，问题如下：{str(e)}")

    # 获取corpus_of_Json_folder中的folder1列表
    folder1_list = os.listdir(corpus_of_Json_folder)
    # 按照文件夹名字升序排序
    folder1_list = sorted(folder1_list)

    # 使用外部进度条处理folder1
    pbar1 = tqdm(folder1_list, desc="总体进度")
    for folder1 in pbar1:
        folder1_path = os.path.join(corpus_of_Json_folder, folder1)
        folder2_list = os.listdir(folder1_path)
        # 按照文件夹名字升序排序
        folder2_list = sorted(folder2_list)

        # 使用内部进度条处理folder1中的folder2
        pbar2 = tqdm(folder2_list, desc=f"正在处理'{folder1}'中的文件夹", leave=False)
        for folder2 in pbar2:
            folder2_path = os.path.join(folder1_path, folder2)
            article_list = os.listdir(folder2_path)

            # 初始化一个空的dataframe来存放folder2的搭配
            folder2_Library_to_be_added = pd.DataFrame(columns=full_columns)

            # 使用内部进度条处理folder2中的每篇文章
            hide_pbar3 = myconfig.hide_pbar3_json2csv  # 设置变量来控制是否显示pbar3

            pbar3 = tqdm(article_list, desc=f"正在处理'{folder2}'中的文章", leave=False, mininterval=60, disable=hide_pbar3)
            for article_file in pbar3:
                # 检查文件扩展名是否为.json
                if not article_file.endswith(".json"):
                    print(f"警告：文件 '{article_file}' 不是json格式，已跳过。")
                    continue  # 跳过当前文件，继续下一个文件

                file_path = os.path.join(folder2_path, article_file)

                # 读取和处理文件
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        article_content = json.load(f)
                        # 使用readArticle函数处理文章
                        articleCollocation = readArticle_fromJSP(article_content)
                        # 合并folder2的结果
                        folder2_Library_to_be_added = mergeLibrary(
                            folder2_Library_to_be_added, articleCollocation
                        )
                except MemoryError:
                    print(f"内存错误：处理文件 '{article_file}' 时内存不足。")
                except Exception as e:
                    print(f"错误：处理文件 '{article_file}' 时出现问题。{str(e)}")

            # 合并folder1的结果
            try:
                CollocationLibrary_to_be_added = mergeLibrary(
                    CollocationLibrary_to_be_added, folder2_Library_to_be_added
                )
            except MemoryError:
                print(f"内存错误：合并folder1 '{folder1}' 的搭配时内存不足。")
                continue  # 跳过当前folder1，继续下一个folder1

        # 暂时保存当前结果
        collocationLibrary_new = CollocationLibrary_to_be_added
        print(f"开始保存截止至 '{folder1}'(包含) 的处理结果")
        try:
            # 使用时间戳保存合并后的库
            timestamp = datetime.now().strftime("%m%d%H%M")

            # 对collocationLibrary_new进行降序排序
            collocationLibrary_new = collocationLibrary_new.sort_values(
                by="搭配频次", ascending=False
            )

            collocationLibrary_new.to_csv(
                os.path.join(
                    collocation_Library_folder,
                    f"collocationLibrary-temp-{timestamp}.csv",
                ),
                index=False,
                encoding="utf-8-sig",
            )
            print(f"截止 '{folder1}' 的处理结果已保存至collocationLibrary-temp-{timestamp}.csv")
        except Exception as e:
            print(f"错误：保存截止 '{folder1}' 的结果时出现问题。{str(e)}")
        # 清空collocationLibrary_new并进行垃圾回收
        collocationLibrary_new = None
        gc.collect()

    # 保存最终结果
    print(f"开始保存所有的处理结果")
    collocationLibrary_new = CollocationLibrary_to_be_added

    # 使用时间戳保存合并后的库
    timestamp = datetime.now().strftime("%m%d%H%M")

    # 对collocationLibrary_new按照搭配频次进行降序排序
    collocationLibrary_new = collocationLibrary_new.sort_values(
        by="搭配频次", ascending=False
    )

    # 语料文件夹名
    partname = myconfig.partname_json2csv
    collocationLibrary_new.to_csv(
        os.path.join(collocation_Library_folder, f"collocationLibrary_{partname}.csv"),
        index=False,
        encoding="utf-8-sig",
    )


def main():
    # # 加载模型
    # HanLP = hanlp.load(
    #     hanlp.pretrained.mtl.CLOSE_TOK_POS_NER_SRL_DEP_SDP_CON_ELECTRA_BASE_ZH
    # )

    # 资源存放文件夹
    rootname = myconfig.rootname_json2csv
    # 语料文件夹名
    partname = myconfig.partname_json2csv
    # 结果存放文件夹（主要用于存放阶段性生成的临时文件）
    resrootname = myconfig.resrootname_json2csv

    # 语料库存放文件夹，注意语料库内部的文件夹结构
    yuliaoku = os.path.join(rootname, partname)
    # 生成的搭配库存放文件夹
    dapeikujieguo = os.path.join(resrootname, partname)
    # 已有的搭配库存储位置
    yiyoudapeiku = myconfig.existed_colLib_path_json2csv

    # 性能检测模块前置
    profiler = cProfile.Profile()
    profiler.enable()

    # 调用搭配库生成代码
    print("")
    print(f"{partname}开始处理。已有搭配库为：[{yiyoudapeiku}]")
    corpus_process_and_merge(yuliaoku, dapeikujieguo, yiyoudapeiku)

    # 性能检测模块后置
    # 时间戳
    timestamp = datetime.now().strftime("%m%d%H%M")
    profiler.disable()
    #是否保存性能分析结果
    saveTag=myconfig.save_profiler
    if(saveTag):
        profiler.dump_stats(f"performance_analysis_4_json2csv_{partname}_{timestamp}.prof")


if __name__ == "__main__":
    main()
