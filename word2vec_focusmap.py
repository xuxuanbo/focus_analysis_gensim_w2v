# -*- coding: utf-8 -*-
from gensim.models import word2vec
import os
import logging
import sys

reload(sys)
sys.setdefaultencoding('utf8')
logging.basicConfig(format='%(asctime)s:%(levelname)s: %(message)s', level=logging.INFO)


sentences = word2vec.Text8Corpus(u"/home/hadoopnew/270万评论crf分词结果")
model = word2vec.Word2Vec(sentences, size=100)# 加载语料
print model


focus_analysis_dir = '/home/hadoopnew/下载/FouceAnalysis-master/关注点标注_new_未延展/'
for (root, dirs, files) in os.walk(focus_analysis_dir):
    for dir in dirs:
        for (sub_root, sub_dirs, sub_files) in os.walk(focus_analysis_dir + dir):
            for i in sub_files:
                fs = open(focus_analysis_dir + dir + '/' + i, 'r')
                for line in fs.readlines():
                    focus_analysis_dir2 = '/home/hadoopnew/下载/FouceAnalysis-master/关注点标注_new_延展/'
                    # 计算两个词的相似度/相关程度，若word2vec模型中没有这个词汇，则简单将该词汇写入延展后的词典随后跳过这个词
                    try:
                        list = model.most_similar(line.strip().decode('utf-8'))
                    except KeyError :
                        ff = open(focus_analysis_dir2 + dir + '/' + i, 'a')
                        ff.write(line )
                        continue
                    ff = open(focus_analysis_dir2 + dir + '/' + i, 'a')
                    ff.write(line)
                    #将list中与该词汇相近的10个词写入新的词典中
                    for word in list:
                        ff.write(word[0]+'\n')
                    ff.close()
                fs.close()
