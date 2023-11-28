import pandas as pd
import cProfile
from datetime import datetime
import os
import gc
from tqdm import tqdm
import CoGenConfig as myconfig

# 定义两种情况的属性列
full_columns = myconfig.full_columns
partial_columns = myconfig.partial_columns

def mergeLibrary(df1, df2):

    # 两个dataframe的列都需要是是full_columns
    if set(df1.columns) == set(full_columns) and set(df2.columns) == set(full_columns):
        merged_df = pd.concat([df1, df2])
        merged_df = merged_df.groupby(partial_columns, as_index=False).sum()

    # 属性列不符合要求，报错并终止函数
    else:
        raise ValueError("两个dataframe的属性列不符合要求")

    return merged_df


def corpus_merge(existed_coLibrary, res_coLibrary):
    '''
    合并多个已存在的搭配库。
    '''
    datacolumns = full_columns

    # 检查corpus_of_Json_folder是否存在
    if not os.path.exists(existed_coLibrary):
        print(f"错误：指定的语料库文件夹{existed_coLibrary}不存在。")
        return

    # 检查collocation_Library_folder是否存在，不存在则创建
    if not os.path.exists(res_coLibrary):
        print(f"指定的结果文件夹{res_coLibrary}不存在，创建该文件夹。")
        os.makedirs(res_coLibrary)

    # 获取所有csv文件
    csv_files = [f for f in os.listdir(existed_coLibrary) if f.endswith(".csv")]
    if not csv_files:
        print(f"错误：指定的语料库文件夹{existed_coLibrary}中没有csv文件。")
        return

    # 按照文件夹名字升序排序
    csv_files = sorted(csv_files)

    # 初始化一个空的dataframe来存放新的搭配
    CollocationLibrary_to_be_added = pd.DataFrame(columns=datacolumns)

    print(f"开始合并{existed_coLibrary}中的语料库：{csv_files}")

    # 使用进度条显示合并进度
    for csv_file in tqdm(csv_files, desc="合并进度", unit="file"):
        file_path = os.path.join(existed_coLibrary, csv_file)
        try:
            current_df = pd.read_csv(file_path)
            CollocationLibrary_to_be_added = mergeLibrary(
                CollocationLibrary_to_be_added, current_df
            )
            del current_df  # 删除当前的dataframe
            gc.collect()  # 清理内存
        except Exception as e:
            print(f"处理文件{csv_file}时出错: {e}")


    # 保存最终结果
    print(f"开始保存所有的处理结果。")
    collocationLibrary_new = CollocationLibrary_to_be_added

    # 时间戳
    timestamp = datetime.now().strftime("%m%d%H%M")
    # 保存合并后的结果
    result_path = os.path.join(
        res_coLibrary, f"coLibrary_merged_result_{timestamp}.csv"
    )
    try:
        # 对collocationLibrary_new按照搭配频次进行降序排序
        collocationLibrary_new = collocationLibrary_new.sort_values(
            by="搭配频次", ascending=False
        )
        print("按照搭配频次进行降序排序，已完成。")

        collocationLibrary_new.to_csv(
            result_path,
            index=False,
            encoding="utf-8-sig",
        )
        print("保存完成。")
    except Exception as e:
        print(f"保存结果时出错: {e}")
    
    return


def main():
    # 资源存放文件夹
    rootname = myconfig.rootname_mergeCoLib
    # 待合并语料库存放文件夹名
    partname = myconfig.partname_mergeCoLib
    # 结果存放文件夹
    resrootname = myconfig.resrootname_mergeCoLib

    # 语料库存放文件夹，注意语料库内部的文件夹结构
    yuliaoku = os.path.join(rootname, partname)
    # 生成的搭配库存放文件夹
    dapeikujieguo = os.path.join(resrootname, partname)

    # 性能检测模块前置
    profiler = cProfile.Profile()
    profiler.enable()

    # 调用搭配库生成代码
    print("")
    print(f"正在合并{yuliaoku}中的语料库，结果将存放至{dapeikujieguo}")

    corpus_merge(yuliaoku, dapeikujieguo)
    # 时间戳
    timestamp = datetime.now().strftime("%m%d%H%M")
    # 性能检测模块后置
    profiler.disable()
    #是否保存性能分析结果
    saveTag=myconfig.save_profiler
    if(saveTag):
        profiler.dump_stats(f"performance_analysis_4_CollocationMerge_{partname}_{timestamp}.prof")

    return


if __name__ == "__main__":
    main()
