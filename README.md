focus_analysis_gensim_w2v
=====
An algorithm mainly bases on word2vec(gensim) and CRF(hanlp) to analysis the focus of the movie fans throught their comments.

Main codes
-----
* hanlp_word_segementation.java

Hadoop project.Using hadoop to get the big data of movie comments from the database and call the crf_segementation of hanlp to do the word segementation.<br><br>
It's ok if you don't know how to use hadoop.You can write a appropriate program using CRF-algorithm to get the same result.

* word2vec_focusmap.py

Using word2vec of gensim to extend the focus_words dictionary.

* focus_analysis.java

Hadoop project.Using hadoop to get the big data of movie comments from the database , recognizes the word of the dictionary in the comments and add it into count.

Operation Environment
-----

* HADOOP(not nessary if your data isn't so big)
* MYSQLDB(not nessary if your data doesn't come from db)
* word2vec,gensim

Operation Instruction
----


