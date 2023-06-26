# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %% [markdown]
# ## Find sentiment in text
# 
# https://colab.research.google.com/github/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/streamlit_notebooks/SENTIMENT_EN.ipynb#scrollTo=4zBXbY_vE2ss

# %%
# JAVA_HOME refers to jdk/bin directory. It is used by a java based application.
import os
jv = os.environ.get('JAVA_HOME', None)
jv

# %% [markdown]
# ### Top-level script environment in Python (__main__)
# 
# A module object is characterized by various attributes. Attribute names are prefixed and post-fixed by double underscore __. The most important attribute of module is __name__. When Python is running as a top level executable code, i.e. when read from standard input, a script, or from an interactive prompt the __name__ attribute is set to '__main__'.
# 

# %%
'module docstring'
print ('name of module:',__name__)


# %%
# However, for an imported module this attribute is set to name of the Python script. For hello.py module

# >>> import hello
# >>> hello.__name__
# hello


# %%
# As seen earlier, the value of __name__ is set to __main__ for top-level module. However, for the imported module it is set to name 
# of a file. Run following script (moduletest.py)

import findspark
print ('name of top level module:', __name__)
print ('name of imported module:', findspark.__name__)

# %% [markdown]
# ### 1. Colab Setup

# %%
import findspark

findspark.init()


# %%
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.0  pyspark-shell'
#  '--packages org.postgresql:postgresql:42.1.1 pyspark-shell'

# %% [markdown]
# PYTHONPATH is an environment variable which you can set to add additional directories where python will look for modules and packages. For most installations, you should not set these variables since they are not needed for Python to run. Python knows where to find its standard library.
# 
# The only reason to set PYTHONPATH is to maintain directories of custom Python libraries that you do not want to install in the global default location (i.e., the site-packages directory).
# 
# The PYTHONPATH variable has a value that is a string with a list of directories that Python should add to the sys.path directory list.
# 
# The main use of PYTHONPATH is when we are developing some code that we want to be able to import from Python, but that we have not yet made into an installable Python package (see: making a Python package).
# 
# Read the following docs to get a better understanding of Python environment variables: http://docs.python.org/using/cmdline.html#environment-variables
# 
# https://www.tutorialspoint.com/How-to-set-python-environment-variable-PYTHONPATH-on-Linux

# %%
get_ipython().system('export PYTHONPATH="/opt/anaconda/envs/pyspark_env/pkgs/spark-nlp-3.0.0-py38_0/lib/python3.8/site-packages/sparknlp/:$PYTHONPATH"')


# %%
get_ipython().system('export PYTHONPATH="/opt/anaconda/envs/pyspark_env/pkgs/spark-nlp-3.0.0-py38_0/lib/python3.8/site-packages/:$PYTHONPATH"')


# %%
get_ipython().system('echo $PYTHONPATH')

# %% [markdown]
# ## Insert spark's third party jar libraries in the path 

# %%
import sys, glob, os
sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))


# %%
sys.path

# %% [markdown]
#  '/home/hadoopuser/.ivy2/jars/com.johnsnowlabs.nlp_spark-nlp_2.12-3.0.0.jar']
# %% [markdown]
# ## Import python's sparknlp wrapper in the path

# %%
# If you want to modify the path to packages from within Python, you can do:

import sys
# sys.path.append('/where/module/lives/')
sys.path.append('/opt/anaconda/envs/pyspark_env/pkgs/spark-nlp-3.0.0-py38_0/lib/python3.8/site-packages/')


# %%
sys.path


# %%
os.environ["TFHUB_CACHE_DIR"] = '/tmp/tfhub'


# %%
import sparknlp.annotator


# %%
import pandas as pd
import numpy as np
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

# %% [markdown]
# ### Setting driver memory is the only way to increase memory in a local spark application.
# 
# "Since you are running Spark in local mode, setting spark.executor.memory won't have any effect, as you have noticed. The reason for this is that the Worker "lives" within the driver JVM process that you start when you start spark-shell and the default memory used for that is 512M. You can increase that by setting spark.driver.memory to something higher, for example 5g" from How to set Apache Spark Executor memory
# 
# %% [markdown]
# ### 2. Start Spark Session

# %%
spark=SparkSession.builder     .appName("ySpark NLP")     .master("local[*]")     .config("spark.driver.memory","7G")     .config("spark.driver.maxResultSize", "5524M")     .config("spark.kryoserializer.buffer.max", "800M")     .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.0")     .getOrCreate()


# %%
sc = spark.sparkContext


# %%
# sc.stop()

# %% [markdown]
# ### 3. Select the DL model and re-run cells below

# %%
#MODEL_NAME='sentimentdl_use_imdb'
MODEL_NAME='sentimentdl_use_twitter'

# %% [markdown]
# ### 4. Some sample examples

# %%
## Generating Example Files ##

text_list = []
if MODEL_NAME=='sentimentdl_use_imdb':
  text_list = [
             """Demonicus is a movie turned into a video game! I just love the story and the things that goes on in the film.It is a B-film ofcourse but that doesn`t bother one bit because its made just right and the music was rad! Horror and sword fight freaks,buy this movie now!""",
             """Back when Alec Baldwin and Kim Basinger were a mercurial, hot-tempered, high-powered Hollywood couple they filmed this (nearly) scene-for-scene remake of the 1972 Steve McQueen-Ali MacGraw action-thriller about a fugitive twosome. It almost worked the first time because McQueen was such a vital presence on the screen--even stone silent and weary, you could sense his clock ticking, his cagey magnetism. Baldwin is not in Steve McQueen's league, but he has his charms and is probably a more versatile actor--if so, this is not a showcase for his attributes. Basinger does well and certainly looks good, but James Woods is artificially hammy in a silly mob-magnet role. A sub-plot involving another couple taken hostage by Baldwin's ex-partner was unbearable in the '72 film and plays even worse here. As for the action scenes, they're pretty old hat, which causes one to wonder: why even remake the original?""",
             """Despite a tight narrative, Johnnie To's Election feels at times like it was once a longer picture, with many characters and plot strands abandoned or ultimately unresolved. Some of these are dealt with in the truly excellent and far superior sequel, Election 2: Harmony is a Virtue, but it's still a dependably enthralling thriller about a contested Triad election that bypasses the usual shootouts and explosions (though not the violence) in favour of constantly shifting alliances that can turn in the time it takes to make a phone call. It's also a film where the most ruthless character isn't always the most threatening one, as the chilling ending makes only too clear: one can imagine a lifetime of psychological counselling being necessary for all the trauma that one inflicts on one unfortunate bystander. Simon Yam, all too often a variable actor but always at his best under To's direction, has possibly never been better in the lead, not least because Tony Leung's much more extrovert performance makes his stillness more the powerful.""",
             """This movie has successfully proved what we all already know, that professional basket-ball players suck at everything besides playing basket-ball. Especially rapping and acting. I can not even begin to describe how bad this movie truly is. First of all, is it just me, or is that the ugliest kid you have ever seen? I mean, his teeth could be used as a can-opener. Secondly, why would a genie want to pursue a career in the music industry when, even though he has magical powers, he sucks horribly at making music? Third, I have read the Bible. In no way shape or form did it say that Jesus made genies. Fourth, what was the deal with all the crappy special effects? I assure you that any acne-addled nerdy teenager with a computer could make better effects than that. Fifth, why did the ending suck so badly? And what the hell is a djin? And finally, whoever created the nightmare known as Kazaam needs to be thrown off of a plane and onto the Eiffel Tower, because this movie take the word "suck" to an entirely new level.""",
             """The fluttering of butterfly wings in the Atlantic can unleash a hurricane in the Pacific. According to this theory (somehow related to the Chaos Theory, I'm not sure exactly how), every action, no matter how small or insignificant, will start a chain reaction that can lead to big events. This small jewel of a film shows us a series of seemingly-unrelated characters, most of them in Paris, whose actions will affect each others' lives. (The six-degrees-of-separation theory can be applied as well.) Each story is a facet of the jewel that is this film. The acting is finely-tuned and nuanced (Audrey Tautou is luminous), the stories mesh plausibly, the humor is just right, and the viewer leaves the theatre nodding in agreement.""",
             """There have been very few films I have not been able to sit through. I made it through Battle Field Earth no problem. But this, This is one of the single worst films EVER to be made. I understand Whoopi Goldberg tried to get of acting in it. I do not blame her. I would feel ashamed to have this on a resume. I belive it is a rare occasion when almost every gag in a film falls flat on it's face. Well it happens here. Not to mention the SFX, look for the dino with the control cables hanging out of it rear end!!!!!! Halfway through the film I was still looking for a plot. I never found one. Save yourself the trouble of renting this and save 90 minutes of your life.""",
             """After a long hard week behind the desk making all those dam serious decisions this movie is a great way to relax. Like Wells and the original radio broadcast this movie will take you away to a land of alien humor and sci-fi paraday. 'Captain Zippo died in the great charge of the Buick. He was a brave man.' The Jack Nicholson impressions shine right through that alien face with the dark sun glasses and leather jacket. And always remember to beware of the 'doughnut of death!' Keep in mind the number one rule of this movie - suspension of disbelief - sit back and relax - and 'Prepare to die Earth Scum!' You just have to see it for yourself.""",
             """When Ritchie first burst on to movie scene his films were hailed as funny, witty, well directed and original. If one could compare the hype he had generated with his first two attempts and the almost universal loathing his last two outings have created one should consider - has Ritchie been found out? Is he really that talented? Does he really have any genuine original ideas? Or is he simply a pretentious and egotistical director who really wants to be Fincher, Tarantino and Leone all rolled into one colossal and disorganised heap? After watching Revolver one could be excused for thinking were did it all go wrong? What happened to his great sense of humour? Where did he get all these mixed and convoluted ideas from? Revolver tries to be clever, philosophical and succinct, it tries to be an intelligent psychoanalysis, it tries to be an intricate and complicated thriller. Ritchie does make a gargantuan effort to fulfil all these many objectives and invests great chunks of a script into existential musings and numerous plot twists. However, in the end all it serves is to construct a severely disjointed, unstructured and ultimately unfriendly film to the audience. Its plagiarism is so sinful and blatant that although Ritchie does at least attempt to give his own spin he should be punished for even trying to pass it off as his own work. So what the audience gets ultimately is a terrible screenplay intertwined with many pretentious oneliners and clumsy setpieces.<br /><br />Revolver is ultimately an unoriginal and bland movie that has stolen countless themes from masterpieces like Fight Club, Usual Suspects and Pulp Fiction. It aims high, but inevitably shots blanks aplenty.<br /><br />Revolver deserves to be lambasted, it is a truly poor film masquerading as a wannabe masterpiece from a wannabe auteur. However, it falls flat on its farcical face and just fails at everything it wants to be and achieve.""",
             """I always thought this would be a long and boring Talking-Heads flick full of static interior takes, dude, I was wrong. "Election" is a highly fascinating and thoroughly captivating thriller-drama, taking a deep and realistic view behind the origins of Triads-Rituals. Characters are constantly on the move, and although as a viewer you kinda always remain an outsider, it\'s still possible to feel the suspense coming from certain decisions and ambitions of the characters. Furthermore Johnnie To succeeds in creating some truly opulent images due to meticulously composed lighting and atmospheric light-shadow contrasts. Although there\'s hardly any action, the ending is still shocking in it\'s ruthless depicting of brutality. Cool movie that deserves more attention, and I came to like the minimalistic acoustic guitar score quite a bit.""",
             """This is to the Zatoichi movies as the "Star Trek" movies were to "Star Trek"--except that in this case every one of the originals was more entertaining and interesting than this big, shiny re-do, and also better made, if substance is more important than surface. Had I never seen them, I would have thought this good-looking but empty; since I had, I thought its style inappropriate and its content insufficient. The idea of reviving the character in a bigger, slicker production must have sounded good, but there was no point in it, other than the hope of making money; it\'s just a show, which mostly fails to capture the atmosphere of the character\'s world and wholly fails to take the character anywhere he hasn\'t been already (also, the actor wasn\'t at his best). I\'d been hoping to see Ichi at a late stage of life, in a story that would see him out gracefully and draw some conclusion from his experience overall; this just rehashes bits and pieces from the other movies, seasoned with more sex and sfx violence. Not the same experience at all."""
             ]
elif  MODEL_NAME=='sentimentdl_use_twitter':
  text_list = [
            """@Mbjthegreat i really dont want AT&amp;T phone service..they suck when it comes to having a signal""",
            """holy crap. I take a nap for 4 hours and Pitchfork blows up my twitter dashboard. I wish I was at Coachella.""",
            """@Susy412 he is working today  ive tried that still not working..... hmmmm!! im rubbish with computers haha!""",
            """Brand New Canon EOS 50D 15MP DSLR Camera Canon 17-85mm IS Lens ...: Web Technology Thread, Brand New Canon EOS 5.. http://u.mavrev.com/5a3t""",
            """Watching a programme about the life of Hitler, its only enhancing my geekiness of history.""",
            """GM says expects announcment on sale of Hummer soon - Reuters: WDSUGM says expects announcment on sale of Hummer .. http://bit.ly/4E1Fv""",
            """@accannis @edog1203 Great Stanford course. Thanks for making it available to the public! Really helpful and informative for starting off!""",
            """@the_real_usher LeBron is cool.  I like his personality...he has good character.""",
            """@sketchbug Lebron is a hometown hero to me, lol I love the Lakers but let's go Cavs, lol""",
            """@PDubyaD right!!! LOL we'll get there!! I have high expectations, Warren Buffet style.""",
            ]


# %%
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("index", IntegerType()),
    StructField("text", StringType())
])

spark_tweetsDF = spark.read.csv('tweets.csv',header=True, schema = schema).dropna()


# %%
spark_tweetsDF.show(5)


# %%


# %% [markdown]
# ### 5. Define Spark NLP pipleline
# %% [markdown]
# Embeddings
# 
# https://nlp.johnsnowlabs.com/docs/en/models
# 
# https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/tfhub_use_en_2.4.0_2.4_1587136330099.zip
# sudo curl -o ./tfhub_en.zip  https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/tfhub_use_en_2.4.0_2.4_1587136330099.zip
# 
# https://github.com/JohnSnowLabs/spark-nlp-models

# %%
# Document Assembler
# As discussed before, each annotator in Spark NLP accepts certain types of columns and
# outputs new columns in another type (we call this AnnotatorType). In Spark NLP, we
# have the following types: Document, token, chunk, pos, word_embeddings, date, entity,
# sentiment, named_entity, dependency, labeled_dependency.

# To get through the process in Spark NLP, we need to get raw data transformed into
# Document type at first. DocumentAssembler() is a special transformer that does this
# for us; it creates the first annotation of type Document which may be used by annotators
# down the road.

# DocumentAssembler() comes from sparknlp.base class and has the following
# settable parameters. See the full list here and the source code here.

#     - setInputCol() -> the name of the column that will be converted. We can specify
#     only one column here. It can read either a String column or an Array[String]
#     - setOutputCol() -> optional : the name of the column in Document type that is
#     generated. We can specify only one column here. Default is ‘document’
#     - setIdCol() -> optional: String type column with id information
#     - setMetadataCol() -> optional: Map type column with metadata information
#     - setCleanupMode() -> optional: Cleaning up options, possible values:

#     disabled: Source kept as original. This is a default.
#     inplace: removes new lines and tabs.
#     inplace_full: removes new lines and tabs but also those which were converted to strings (i.e. \n)
#     shrink: removes new lines and tabs, plus merging multiple spaces and blank lines to a single space.
#     shrink_full: remove new lines and tabs, including stringified values, plus shrinking spaces and blank lines.

documentAssembler = DocumentAssembler()    .setInputCol("text")    .setOutputCol("document")

# TensorFlow Hub -> tfhub
# use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
use = UniversalSentenceEncoder.load("/home/hadoopuser/nlp/tfhub/") .setInputCols(["document"]) .setOutputCol("sentence_embeddings")


# sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en")\
#     .setInputCols(["sentence_embeddings"])\
#     .setOutputCol("sentiment")

# nlpPipeline = Pipeline(
#       stages = [
#           documentAssembler,
#           use,
#           sentimentdl
#       ])


# %%
sparknlp.version()


# %%
sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en")    .setInputCols(["sentence_embeddings"])    .setOutputCol("sentiment")

nlpPipeline = Pipeline(
      stages = [
          documentAssembler,
          use,
          sentimentdl
      ])

# %% [markdown]
# ### 6. Run the pipeline

# %%
empty_df = spark.createDataFrame([['']]).toDF("text")

pipelineModel = nlpPipeline.fit(empty_df)

df = spark.createDataFrame(pd.DataFrame({"text":text_list}))
result = pipelineModel.transform(df)


# %%
empty_df1 = spark.createDataFrame([['']]).toDF("text")

pipelineModel = nlpPipeline.fit(empty_df1)

# df = spark.createDataFrame(pd.DataFrame({"text":text_list}))
result1 = pipelineModel.transform(spark_tweetsDF)


# %%
result1.printSchema()


# %%
result1.select('text', 'sentiment').toPandas()


# %%
from pyspark.sql.functions import col
# result1.select('text', col('sentiment')[0]).toPandas().style.set_properties(subset=['sentiment'], **{'width': '300px'})
# result1.select('text', col('sentiment')[0]['result'], col('sentiment')[0]['metadata']).show(truncate = False)

result1_f = result1.select('text', 'sentiment').withColumn('result', col('sentiment')[0]['result']).withColumn('negative', col('sentiment')[0]['metadata']['negative'])         .withColumn('positive', col('sentiment')[0]['metadata']['positive'])         .drop('sentiment').toPandas()
result1_f.style.set_properties(subset=['text'], **{'width': '300px'})


# %%
result.select('text','sentiment').toPandas().style.set_properties(subset=[['text','sentiment']][0], **{'width': '400px'})


# %%
result.select('sentiment').show(truncate=False)

# %% [markdown]
# ### 7. Visualize results

# %%
result.select(F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols")) .select(F.expr("cols['0']").alias("document"),
        F.expr("cols['1']").alias("sentiment")).show(truncate=False)


# %%
data = [['study is going on as usual'], ['Goodmorning to everyone'], ['I am very sad today.'], ['Geeks For Geeks is the best portal for              the computer science engineering students.']]
columns = ['text']

data = [[' '.join(text[0].split())] for text in data]
print(data)

# df = spark.createDataFrame(rdd).toDF(*columns)
df1 = spark.createDataFrame(data=data,schema=columns)
df1.show(truncate=False)


# %%
df1.show()


# %%
empty_df1 = spark.createDataFrame([['']]).toDF("text")

pipelineModel = nlpPipeline.fit(empty_df1)

# df1 = spark.createDataFrame(pd.DataFrame({"text":text_list}))
result1 = pipelineModel.transform(df1)


# %%
result1.select('sentiment').show(truncate=False)


# %%
result1.select(F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols")) .select(F.expr("cols['0']").alias("document"),
        F.expr("cols['1']").alias("sentiment")).show(truncate=False)


# %%



