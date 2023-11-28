import hanlp
from hanlp_common.document import Document
import pandas as pd
from tqdm import tqdm
import time
import os
from datetime import datetime
import cProfile
import json
import re
import CoGenConfig as myconfig

#本版本的更新要点：1.修改了分句规则，分句时句子终结符不再出现。2.更换了使用的hanlp模型

 # 定义一个分隔符，用于替换缩写
_SEPARATOR = r'@' 
# 定义一个句子识别的正则表达式，能够找到句尾符号或者行尾
_RE_SENTENCE = re.compile(r'(\S.+?[.!?])(?=\s+|$)|(\S.+?)(?=[\n]|$)', re.UNICODE)
# 定义一个匹配缩写的正则表达式，如“Mr.”
_AB_SENIOR = re.compile(r'([A-Z][a-z]{1,2}\.)\s(\w)', re.UNICODE)
# 定义一个匹配连续缩写的正则表达式，如“U.S.A.”
_AB_ACRONYM = re.compile(r'(\.[a-zA-Z]\.)\s(\w)', re.UNICODE)
# 定义用于还原缩写的正则表达式
_UNDO_AB_SENIOR = re.compile(r'([A-Z][a-z]{1,2}\.)' + _SEPARATOR + r'(\w)', re.UNICODE)
_UNDO_AB_ACRONYM = re.compile(r'(\.[a-zA-Z]\.)' + _SEPARATOR + r'(\w)', re.UNICODE)

def _replace_with_separator(text, separator, regexs):
    # 将匹配到的模式替换为分隔符
    replacement = r"\1" + separator + r"\2"
    result = text
    for regex in regexs:
        result = regex.sub(replacement, result)
    return result

def custom_split_sentence(text, best=True):
    # 使用正则表达式替换所有的句子终结符为换行符，包括空格
    text = re.sub(r'([。！!？?,，:：；;]|\s)', r"\n", text)
    # 对于六个连续句点构成的省略号，替换为换行符
    text = re.sub(r'(\.{6})', r"\n", text)
    # 对于两个连续省略号字符构成的省略号，替换为换行符
    text = re.sub(r'(…{2})', r"\n", text)

    for chunk in text.split("\n"):  # 通过换行符来分割文本
        chunk = chunk.strip()
        if not chunk:  # 如果行为空，则忽略
            continue
        if not best:  # 如果不需要最佳分割，则直接返回结果
            yield chunk
            continue
        # 处理缩写，防止它们被错误地分割
        processed = _replace_with_separator(chunk, _SEPARATOR, [_AB_SENIOR, _AB_ACRONYM])
        sents = list(_RE_SENTENCE.finditer(processed))  # 查找所有句子
        if not sents:  # 如果没有找到句子，则直接返回
            yield chunk
            continue
        for sentence in sents:  # 对找到的每个句子，还原缩写后返回
            sentence = _replace_with_separator(sentence.group(), r" ", [_UNDO_AB_SENIOR, _UNDO_AB_ACRONYM])
            yield sentence


def readArticle_withHanLP_tok_fine(article, HanLP):
    # 执行的任务标签
    tok_tasks = "tok/fine"
    pos_tasks = "pos/pku"
    dep_tasks = "dep"

    # 为hanlp添加任务，先分句，然后分词，词性标注和依存句法分析
    articleTok = (
        hanlp.pipeline()
        .append(custom_split_sentence)  # 自定义规则分句
        .append(HanLP, tasks=[tok_tasks, pos_tasks, dep_tasks])  # 分词，词性标注，句法分析
    )

    # 执行hanlp任务，得到结果
    SSentencList = articleTok(article)

    return SSentencList




def articleFolder_2jsonFolder(HanLP, articleFolder, jsonFolder):


    # 检查文章所在的文件夹是否存在
    if not os.path.exists(articleFolder):
        os.makedirs(articleFolder)

    # 检查json结果存放的文件夹是否存在
    if not os.path.exists(jsonFolder):
        os.makedirs(jsonFolder)

    # 获取文章文件夹中的文件列表
    file_list = os.listdir(articleFolder)

    for article_file in file_list:
        # 检查文件扩展名是否为.txt
        if not article_file.endswith(".txt"):
            print(f"警告：文件 '{article_file}' 不是txt格式,已跳过。")
            continue  # 跳过当前文件，继续下一个文件

        file_path = os.path.join(articleFolder, article_file)


        # 读取和处理文件
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                article_content = f.read()
                # 使用readArticle_withHanLP函数处理文章
                article_result = readArticle_withHanLP_tok_fine(article_content, HanLP)
                # 将处理结果保存为json文件
                result_file_path = os.path.join(jsonFolder, f"{article_file}.json")
                with open(result_file_path, "w", encoding="utf-8") as result_file:
                    json.dump(article_result, result_file, ensure_ascii=False, indent=4)
        except RecursionError:
            print(f"递归错误：处理文件 '{article_file}' 时出现问题。")
            continue  # 跳过当前文件，继续处理下一个文件
        except RuntimeError as e:
            if "CUDA out of memory" in str(e):
                print(f"错误：处理文件 '{article_file}' 时出现问题。CUDA内存不足，跳过此文件。")
                continue  # 跳过当前文件，继续处理下一个文件
            else:
                raise Exception("Unexpected RuntimeError")  # 如果是其他类型的RuntimeError，重新抛出
        except Exception as e:
            print(f"错误：处理文件 '{article_file}' 时出现问题。{str(e)}")
            continue  # 跳过当前文件，继续处理下一个文件





def corpus_2json(HanLP,corpus_folder,corpus_json_folder):


    # 检查corpus_folder是否存在，不存在则报错
    if not os.path.exists(corpus_folder):
        raise FileNotFoundError(f"{corpus_folder} 不存在！")

    # 检查corpus_json_folder是否存在，不存在则创建
    if not os.path.exists(corpus_json_folder):
        os.makedirs(corpus_json_folder)

    # 如果corpus_json_folder存在，获取其内部的文件夹列表
    corpus_folder_1_list_done = []
    if os.path.exists(corpus_json_folder):
        # 获取文件夹列表并去除尾部的“-json”
        corpus_folder_1_list_done = [
            f.replace("-json", "")
            for f in os.listdir(corpus_json_folder)
            if os.path.isdir(os.path.join(corpus_json_folder, f))
        ]
        print(f"以下文件夹已经处理过了，本次处理会跳过：{corpus_folder_1_list_done}")

    corpus_folder_1_list_error=[]
    print(f"以下文件夹出现bug,本次处理会跳过：{corpus_folder_1_list_error}")
    corpus_folder_1_list_done.extend(corpus_folder_1_list_error)


    # 获取corpus_folder中所有的corpus_folder_1
    corpus_folder_1_list = [
        f
        for f in os.listdir(corpus_folder)
        if os.path.isdir(os.path.join(corpus_folder, f))
        and f not in corpus_folder_1_list_done
    ]
    # 按照文件夹名字升序排序
    corpus_folder_1_list = sorted(corpus_folder_1_list)
    
    with tqdm(total=len(corpus_folder_1_list), desc="总体进度", position=0) as pbar_outer:
        for folder_1 in corpus_folder_1_list:
            json_folder_1 = os.path.join(corpus_json_folder, folder_1 + "-json")
            if not os.path.exists(json_folder_1):
                os.makedirs(json_folder_1)

            corpus_folder_1_path = os.path.join(corpus_folder, folder_1)
            # 获取corpus_folder_1中的所有corpus_folder_2
            corpus_folder_2_list = [
                f
                for f in os.listdir(corpus_folder_1_path)
                if os.path.isdir(os.path.join(corpus_folder_1_path, f))
            ]

            with tqdm(
                total=len(corpus_folder_2_list),
                desc=f"{folder_1}进度",
                position=1,
                leave=False,
            ) as pbar_inner:
                for folder_2 in corpus_folder_2_list:
                    json_folder_2 = os.path.join(json_folder_1, folder_2 + "-json")
                    if not os.path.exists(json_folder_2):
                        os.makedirs(json_folder_2)

                    articleFolder_2jsonFolder(
                        HanLP, 
                        os.path.join(corpus_folder_1_path, folder_2),
                        json_folder_2,
                    )
                    pbar_inner.update(1)  # 更新内部进度条

            pbar_outer.update(1)  # 更新外部进度条



def main():
    # # 加载模型(旧)
    # HanLP = hanlp.load(
    #     hanlp.pretrained.mtl.CLOSE_TOK_POS_NER_SRL_DEP_SDP_CON_ELECTRA_BASE_ZH
    # )
    # 加载模型
    HanLP = hanlp.load(
        hanlp.pretrained.mtl.CLOSE_TOK_POS_NER_SRL_UDEP_SDP_CON_ELECTRA_SMALL_ZH
    )


    #资源存放根文件夹
    rootname = myconfig.rootname_txt2json
    #当前处理的语料文件夹名
    partname = myconfig.partname_txt2json
    #结果存放文件夹
    resrootname = myconfig.resrootname_txt2json
    #当前处理的语料库结果json存放文件夹名
    respartname=partname+"_json"

    #语料库存放文件夹，注意语料库内部的文件夹结构
    yuliaoku = os.path.join(rootname, partname)
    #生成的json文件存放文件夹
    jieguo = os.path.join(resrootname, respartname)



    profiler = cProfile.Profile()
    profiler.enable()

    corpus_2json(HanLP,yuliaoku,jieguo)

    # 时间戳
    timestamp = datetime.now().strftime("%m%d%H%M")
    # 性能检测模块后置
    profiler.disable()
    profiler.dump_stats(f"performance_analysis_4_txt2json_{partname}_{timestamp}.prof")



if __name__ == "__main__":
    main()
