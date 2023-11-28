import pandas as pd
import os
from datetime import datetime
import cProfile
import CoGenConfig as myconfig
from tqdm import tqdm


def save_to_txt(df, file_path):
    """
    将DataFrame保存为特定格式的txt文件。

    :param df: 要保存的DataFrame。
    :param file_path: txt文件的保存路径。
    """
    n=myconfig.Freq_n_ColLibProcessing
    sI=myconfig.search_index_ColLibProcessing
    plusStr=f"_n={n}_sI={sI}"

    file_path = file_path + plusStr+".txt"

    # 按照词语1升序，词语2升序排序
    df = df.sort_values(by=["词语1", "词语2"])
    
    # 将数据格式化为字符串
    formatted_data = df.apply(
        lambda row: f"{row['词语1']} {row['词语2']}:{row['搭配频次']}", axis=1
    )

    # 保存为txt文件
    with open(file_path, "w", encoding="utf-8") as file:
        for line in formatted_data:
            file.write(line + "\n")


def process_colib_data(
    colib_file,
    result_file,
    aux_result_file=None,
    Freq_n=10,
    CoLib_exclude_dep=set(),
    CoLib_exclude_pos=set(),
):
    """
    处理搭配库数据。

    :param colib_file: 搭配库文件地址。
    :param result_file: 结果存放的文件地址。
    :param aux_result_file: 辅助结果存放的文件地址（可选）。
    :param Freq_n: 搭配频次的阈值。
    :param CoLib_need_dep: 需要保留的词语间依存关系集合。
    :param CoLib_need_pos: 需要保留的词性集合。
    """
    try:
        # 检查结果文件是否已存在
        if os.path.exists(result_file):
            raise FileExistsError(f"结果文件 '{result_file}' 已存在。")
        if aux_result_file and os.path.exists(aux_result_file):
            raise FileExistsError(f"辅助结果文件 '{aux_result_file}' 已存在。")

        # 读取搭配库文件
        if not os.path.exists(colib_file):
            raise FileNotFoundError(f"搭配库文件 '{colib_file}' 不存在。")
        colib_data = pd.read_csv(colib_file)

        # 过滤操作
        colib_data = colib_data[colib_data["搭配频次"] >= Freq_n]
        # 根据词语间依存关系黑名单过滤
        colib_data = colib_data[~colib_data["词语间依存关系"].isin(CoLib_exclude_dep)]
        # 根据词性黑名单过滤：如果两个词语中任意一个词在黑名单中则不要该搭配
        colib_data = colib_data[
            ~(
                colib_data["词语1词性"].isin(CoLib_exclude_pos)
                | colib_data["词语2词性"].isin(CoLib_exclude_pos)
            )
        ]

        print("过滤操作已完成。")

        # 删除不需要的列并合并
        colib_data = colib_data.drop(columns=["词语1词性", "词语2词性", "词语间依存关系"])
        colib_data = colib_data.groupby(["词语1", "词语2"]).sum().reset_index()

        # 添加辅助计算列
        colib_data["is_del"] = 0

        # 应用规则
        colib_data = rule_1(colib_data)
        print("规则1已应用完成。")
        colib_data = rule_2(colib_data)
        print("规则2已应用完成。")

        # # 分离和保存表格csv——ver
        # if aux_result_file:
        #     aux_data = colib_data[colib_data["is_del"] == 1].drop(columns=["is_del"])
        #     aux_data.sort_values(by="搭配频次", ascending=False).to_csv(
        #         aux_result_file+".csv", index=False, encoding="utf-8-sig"
        #     )

        # result_data = colib_data[colib_data["is_del"] == 0].drop(columns=["is_del"])
        # result_data.sort_values(by="搭配频次", ascending=False).to_csv(
        #     result_file+".csv", index=False, encoding="utf-8-sig"
        # )

        # 使用txt
        # 分离并保存表格
        if aux_result_file:
            aux_data = colib_data[colib_data["is_del"] == 1].drop(columns=["is_del"])
            save_to_txt(aux_data, aux_result_file)

        result_data = colib_data[colib_data["is_del"] == 0].drop(columns=["is_del"])
        save_to_txt(result_data, result_file)

    except Exception as e:
        print(f"处理过程中发生错误: {e}")


def rule_1(df):
    """
    应用规则1：如果词语1和词语2均为单个字符，则is_del置为1。
    """
    df["is_del"] = df.apply(
        lambda row: 1
        if len(row["词语1"]) == 1 and len(row["词语2"]) == 1
        else row["is_del"],
        axis=1,
    )
    return df


def rule_2(df):
    """
    处理DataFrame，标记符合特定条件的搭配为删除。

    参数:
    df (DataFrame): 包含列 '词语1', '词语2' 的DataFrame。

    返回:
    DataFrame: 处理后的DataFrame。 
    """
    # 定义邻近搜索范围
    search_index = myconfig.search_index_ColLibProcessing
    print(f"当前邻近搜索范围为：{search_index}")
    # 函数，检查a是否是b的子句
    is_subclause = lambda a, b: a in b
    # 第一步：按照 '词语2' 排序并处理
    df.sort_values(by=["词语2", "词语1"], inplace=True)
    #重置索引
    df.reset_index(drop=True, inplace=True)
    for i in tqdm(range(len(df)), desc="Processing '词语2'", mininterval=10):
        if len(df.iloc[i]["词语1"]) == 1:  # 如果 '词语1' 是单个汉字
            # 向上搜索
            for j in range(i - 1, max(-1, i - search_index - 1), -1):
                if df.iloc[j]["词语2"] != df.iloc[i]["词语2"]:
                    break
                if is_subclause(df.iloc[i]["词语1"], df.iloc[j]["词语1"]):
                    df.at[i, "is_del"] = 1
                    break
            # 向下搜索
            for j in range(i + 1, min(len(df), i + search_index + 1)):
                if df.iloc[j]["词语2"] != df.iloc[i]["词语2"]:
                    break
                if is_subclause(df.iloc[i]["词语1"], df.iloc[j]["词语1"]):
                    df.at[i, "is_del"] = 1
                    break
    # 第二步：按照 '词语1' 排序并处理
    df.sort_values(by=["词语1", "词语2"], inplace=True)
    #重置索引
    df.reset_index(drop=True, inplace=True)
    for i in tqdm(range(len(df)), desc="Processing '词语1'", mininterval=10):
        if len(df.iloc[i]["词语2"]) == 1:  # 如果 '词语2' 是单个汉字
            # 向上搜索
            for j in range(i - 1, max(-1, i - search_index - 1), -1):
                if df.iloc[j]["词语1"] != df.iloc[i]["词语1"]:
                    break
                if is_subclause(df.iloc[i]["词语2"], df.iloc[j]["词语2"]):
                    df.at[i, "is_del"] = 1
                    break
            # 向下搜索
            for j in range(i + 1, min(len(df), i + search_index + 1)):
                if df.iloc[j]["词语1"] != df.iloc[i]["词语1"]:
                    break
                if is_subclause(df.iloc[i]["词语2"], df.iloc[j]["词语2"]):
                    df.at[i, "is_del"] = 1
                    break
    return df


def main():
    rootname = myconfig.rootname_ColLibProcessing
    partname = myconfig.partname_ColLibProcessing
    resrootname = myconfig.resrootname_ColLibProcessing
    resname = "res_" + partname

    yuliaoku = os.path.join(rootname, partname) + ".csv"
    dapeikujieguo = os.path.join(resrootname, resname)

    if myconfig.need_del:
        resname = "del_" + resname
        ewaijieguo = os.path.join(resrootname, resname)
    else:
        ewaijieguo = ""

    profiler = cProfile.Profile()
    profiler.enable()

    print("")
    print(f"{yuliaoku}开始处理。结果将保存至{dapeikujieguo}.")
    if myconfig.need_del:
        print(f"删除结果将保存至{ewaijieguo}.")

    process_colib_data(
        yuliaoku,
        dapeikujieguo,
        ewaijieguo,
        Freq_n=myconfig.Freq_n_ColLibProcessing,
        CoLib_exclude_dep=myconfig.CoLib_exclude_dep_ColLibProcessing,
        CoLib_exclude_pos=myconfig.CoLib_exclude_pos_ColLibProcessing,
    )

    # 时间戳
    timestamp = datetime.now().strftime("%m%d%H%M")
    profiler.disable()
    #是否保存性能分析结果
    saveTag=myconfig.save_profiler
    if(saveTag):
        profiler.dump_stats( 
            f"performance_analysis_4CoLibProcessing_{partname}_{timestamp}.prof"
        )


if __name__ == "__main__":
    main()
