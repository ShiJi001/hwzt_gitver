#使用方法：
# import CoGenConfig 
# CoGenConfig.full_columns

# 定义搭配库表格两种情况的属性列
full_columns = ["词语1", "词语2", "词语1词性", "词语2词性", "词语间依存关系", "搭配频次"]
partial_columns = ["词语1", "词语2", "词语1词性", "词语2词性", "词语间依存关系"]

# 定义关键词库表格两种情况的属性列
words_full_columns = ["关键词", "词性", "词频"]
words_partial_columns = ["关键词", "词性"]

noun_pos = {'an', 'n', 'vn'}


#-----------------------------------------------------------------------------------------------------------
#txt to json

#资源存放文件夹
rootname_txt2json = r"/home/xt/workplace/hwzt/code/res4code_231127"
#语料文件夹名，该文件夹位于上述文件夹目录下
partname_txt2json = "yulu_231128"
#结果存放文件夹，本环节中一般应与资源存放文件夹类似
resrootname_txt2json = r"/home/xt/workplace/hwzt/code/res4code_231127"
#-----------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------
#json to csv

#资源存放文件夹
rootname_json2csv = r"/home/xt/workplace/hwzt/code/res4code_231127"
#语料文件夹名，该文件夹位于上述文件夹目录下
partname_json2csv = "yulu_231128_json"
#结果存放文件夹，（主要用于存放阶段性生成的临时文件）
resrootname_json2csv = r"/home/xt/workplace/hwzt/res/res4code_231127"
#已有搭配库的位置
existed_colLib_path_json2csv=r""
#是否隐藏最底层进度条，当数据量大时建议为true
hide_pbar3_json2csv = True 

#-----------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------
#合并搭配库

#资源存放文件夹
rootname_mergeCoLib = r"/home/xt/workplace/hwzt/code/res4code_231105"
#语料文件夹名，该文件夹位于上述文件夹目录下
partname_mergeCoLib = "to_be_merged_1123"
#结果存放文件夹，（主要用于存放阶段性生成的临时文件）
resrootname_mergeCoLib = r"/home/xt/workplace/hwzt/res/res4code_231105"
#已有搭配库的位置
existed_colLib_path_json2csv=r""
#-----------------------------------------------------------------------------------------------------------



#-----------------------------------------------------------------------------------------------------------
#从json中提取关键词库

#资源存放文件夹
rootname_json2words = r"/home/xt/workplace/hwzt/code/res4code_231127"
#语料文件夹名，该文件夹位于上述文件夹目录下
partnames_json2words = [
    "yulu_231128_json"
]
#本次运行的名字
partname_json2words="yulu_231128_json"
#结果存放文件夹，（主要用于存放阶段性生成的临时文件）
resrootname_json2words = r"/home/xt/workplace/hwzt/res/res4code_231127"
#已有搭配库的位置
existed_colLib_path_json2words=r""

# 纳入统计的词性列表
# 需要学习的词性列表（切割规则细分）：
# 【a, ad, an,d,i,j,l,n,ns,nz,v,vd,vn,z】

# 需要学习的词性列表（切割规则粗分）：
# 【a, ad, an,d,i,j,l,ns,nt,nz,v,vd,vn,z】
# 细分词性列表
wordsLib_need_pos_fine = ["a","ad","an","d","i","j","l","n","ns","nz","v","vd","vn","z",]
#-----------------------------------------------------------------------------------------------------------



#-----------------------------------------------------------------------------------------------------------
#搭配库处理

#语料库地址
rootname_ColLibProcessing = r"/home/xt/workplace/hwzt/res/res4code_231105/to_be_merged_1123"
#语料文件名，该文件位于上述文件夹目录下,应当是一个csv格式文件
partname_ColLibProcessing = "coLibrary_merged_result_11231948"
#结果存放文件夹，（主要用于存放阶段性生成的临时文件）
resrootname_ColLibProcessing = r"/home/xt/workplace/hwzt/res/res4code_231105/to_be_merged_1123"
#是否需要删除部分？
need_del=True
#过滤阈值,大于等于阈值的数据保留
Freq_n_ColLibProcessing=10
# 定义邻近搜索范围
search_index_ColLibProcessing= 60
#搭配与词性黑名单
CoLib_exclude_dep_ColLibProcessing={"punct","mark","etc","discourse","aux:asp","aux:ba","nmod:poss","cop"}
CoLib_exclude_pos_ColLibProcessing={"nr","ns","w","t"}
#-----------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------
#关键词库处理

#语料库地址
CSV_path_WFLibProcessing = r"/home/xt/workplace/hwzt/res/res4code_231105/wordsFreq_20231119/wordFreqLibrary-11200357.csv"
#语料文件夹名，该文件夹位于上述文件夹目录下
txt_path_WFLibProcessing = r"/home/xt/workplace/hwzt/code/res4code_231105/04-新关键词（没逗号分隔）1011.txt"

#结果存放文件夹
resrootname_WFLibProcessing = r"/home/xt/workplace/hwzt/res/res4code_231105/wordsFreq_20231119"


#过滤阈值,大于等于阈值的数据保留
Freq_n_WFLibProcessing=[40,55,69,100]
#词性黑名单(人名，地名，机构团体，标点符号，时间)
WFLib_exclude_pos_WFLibProcessing={"nr","ns","nt","w","t"}
#-----------------------------------------------------------------------------------------------------------