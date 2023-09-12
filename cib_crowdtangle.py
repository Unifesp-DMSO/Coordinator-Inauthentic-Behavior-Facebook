#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-**-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* 

											"cib.py"
											*********
								coordinated inauthentic behavior
								********************************

		Developed by: Wilson  Ceron		e-mail: wilsonseron@gmail.com 		Date: 15/08/2021
								

-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-**-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* 
'''

import csv
import re
import sys, os
import time
import pandas as pd

from datetime import date,datetime,timedelta
from collections import Counter
import argparse
from argparse import RawTextHelpFormatter

sys.path.insert(1, 'SNAPKG')

from Common.classes.pre_processor import pre_processor


import warnings
warnings.filterwarnings("ignore")

n_parel_posts	=	3	
interval		=	30

def find_cib(df_fake, post):

	cib			=	-1
	new_cib		=	0
	new_cut		=	[]
	start_date	=	post['created_at']
	end_date	=	post['created_at']+timedelta(seconds=interval)
	old_date	=	end_date
	
	
	while True:
		
		mask 		= (df_fake['created_at'] >=start_date) & (df_fake['created_at'] <= end_date)
		df_cut		=	df_fake.loc[mask]
		df_cut		=	df_cut.sort_values('created_at',ascending=True)
		
		if len(df_cut.index) <=1:
			df_fake = df_fake.drop(df_cut[mask].index)
			break
		
		for index,row in df_cut.iterrows(): 
			if ((post['pre_Message']	==	row['pre_Message'] 		and len(row['pre_Message']) 	!= 0 and row['pre_Message']		!=	"nan"	) or 
					(post['pre_Image_Text']	==	row['pre_Image_Text'] 	and len(row['pre_Image_Text']) 	!= 0 and row['pre_Image_Text']	!=	"nan"	) or 
					(post['pre_Description']==	row['pre_Description'] 	and len(row['pre_Description']) != 0 and row['pre_Description']	!=	"nan"	) or
					(post['Link']			==	row['Link'] 			and len(row['Link']) 			!= 0 and row['Link']			!=	"nan"	) ):
					#print(post['pre_Image_Text'])
					#print(row['pre_Image_Text'])
					pass
			else:		
				df_cut = df_cut.drop(index)
		
		
		if len(df_cut.index) <=0:
			break

		new_cib	=	len(df_cut.index)		
		if cib < new_cib:
			old_date	=	end_date
			end_date	=	max(df_cut['created_at'])+timedelta(seconds=interval)
			cib			=	new_cib	
			new_cut		=	df_cut.copy()
		else:
			df_cut		=	new_cut
			df_fake 	=	df_fake.drop(df_cut[mask].index)
			break	
	
	return 	df_fake,df_cut


def create_graphs(df,fake_posts,path):

	graph_path		=	"./fakes/graph_files"
	os.makedirs(graph_path, exist_ok = True)


	f_fake		=	open(graph_path+"/fake_groups_edges.csv","+w")
	f_fake.write("source,target,Total_Interactions,Type\n")
	fakes_name	=	[]
	all_groups	=	[]
	edges_list	=	[]
	for fake in fake_posts:
		name		=		'\"'	+	fake.get('fake')	+	'\"'
		fakes_name.append(name)
		posts_list	=		fake.get('posts_ids')
		for posts_id in posts_list:
			for i in posts_id:
				
				source		=	str(df.loc[df['URL'].str.contains(i)]['Facebook_Id'].iloc[0])
				all_groups.append(source)

				source				=	'\"'	+	source	+	'\"'		
				total_Interactions	=	str(df.loc[df['URL'].str.contains(i)]['Total_Interactions'].iloc[0]).split(",")[0]
				p_type				=	'\"'	+	df.loc[df['URL'].str.contains(i)]['Type'].iloc[0]	+	'\"'
				f_fake.write(	name	+","+	source	+","+	str(total_Interactions)	+","+	str(p_type)	+"\n"	)
				
				for j in posts_id:
					target	=	'\"'	+	str(df.loc[df['URL'].str.contains(j)]['Facebook_Id'].iloc[0])	+	'\"'
					if source!=target:
						edges_list.append((source,target))
						
	f_fake.close()


	edges_dict		=	dict(Counter(edges_list))	
	f_echo	=	open(graph_path+"/echo_chambers_edges.csv","+w")	
	f_echo.write("source,target,weight\n")
	for k,v in edges_dict.items():
		f_echo.write(	str(k[0])	+","+	str(k[1])	+","+	str(v)	+"\n"	)	
	f_echo.close()



	f_fake		=	open(graph_path+"/fake_groups_nodes.csv","+w")
	f_echo		=	open(graph_path+"/echo_chambers_nodes.csv","+w")
	
	f_fake.write("id,label,type\n")
	f_echo.write("id,label\n")
	
	all_groups	=	list(set(all_groups))
	for group in all_groups:

		label	=	format_label(	df.loc[df['URL'].str.contains(group)]['Group_Name'].iloc[0]	)
		group				=	'\"'	+	str(group)	+	'\"'		
		
		f_fake.write(	str(group)	+","+	label	+",Group\n")
		f_echo.write(	str(group)	+","+	label	+"\n")

	fakes_name = list(set(fakes_name))		
	for fake in fakes_name:
		f_fake.write(fake+","+fake+",Fake\n")
	
	f_fake.close()	
	f_echo.close()


def format_label(group_name):
	label	=	re.sub('\W+',' ', group_name )
	
	label	=	 '\"'	+	label	+	'\"'
	
	return label

def main(argv):

	df						=	pd.read_csv("2018-2021.csv")
	df.columns				=	[c.replace(' ', '_') for c in df.columns]
	df['created_at']		=	pd.to_datetime(df.Post_Created.astype(str).str[:-4], format="%Y-%m-%d %H:%M:%S")



	# Elimina as colunas não utilizadas
	df = df.drop(columns=['Post_Created','User_Name','Page_Category','Page_Admin_Top_Country','Page_Description','Page_Created','Likes_at_Posting'])
	df = df.drop(columns=['Followers_at_Posting','Post_Created_Date','Post_Created_Time','Video_Share_Status','Is_Video_Owner?','Post_Views','Total_Views'])
	df = df.drop(columns=['Total_Views_For_All_Crossposts','Video_Length','Sponsor_Id','Sponsor_Name','Sponsor_Category','Final_Link'])
	df = df.drop(columns=['Total_Interactions_(weighted__—__Likes_1x_Shares_1x_Comments_1x_Love_1x_Wow_1x_Haha_1x_Sad_1x_Angry_1x_Care_1x_)'])
	df['Fake']	="F20200605"


	#converte coluna pra string
	df.Fake = df.Fake.astype(str)
	df.Message = df.Message.astype(str)
	df.Image_Text = df.Image_Text.astype(str)
	df.Description = df.Description.astype(str)
	df.URL = df.URL.astype(str)

	#Aplica o preprocessamento
	df['pre_Message']		=	df['Message'].apply(	lambda x: set(pre_processor(x	, stop_words=True, keep_hashtags=True, keep_mentions=True,language = "pt")))
	df['pre_Image_Text']	=	df['Image_Text'].apply(	lambda x: set(pre_processor(x	, stop_words=True, keep_hashtags=True, keep_mentions=True,language = "pt")))
	df['pre_Description']	=	df['Description'].apply(lambda x: set(pre_processor(x	, stop_words=True, keep_hashtags=True, keep_mentions=True,language = "pt")))


	fakes					=	list(df['Fake'].unique())
	fake_posts			=	[]	
	f						=	open("cib_results.csv","+w")
	f.write("Fake_index,N_posts,N_cib,n_posts_cib\n")	

	dfs_total			=	[]		
	for fake in fakes:
		try:
			cib_total		=	0
			cib_total_posts	=	0
			n_posts			=	0
			mask 			=	(df['Fake'].str.contains(fake))
			df_fake			=	df.loc[mask]
			df_fake			=	df_fake.sort_values('created_at',ascending=True)
			dfs				=	[]
			i				=	1
			posts_id		=	[]
			cut_nposts		=	0
			n_posts			=	len(df_fake.index)
			cib				=	False
			print("Processing fake: "+fake +" with " +str(len(df_fake.index))+ " posts"  )
			for index,post in df_fake.iterrows():
				df_fake,df_cut	=	find_cib(df_fake, post)
				print(i)
				if len(df_cut.index) >= n_parel_posts:
					cib				=	True
					fake_path		=	"./fakes/"+fake
					os.makedirs(fake_path, exist_ok = True)
					df_cut['cib']	=	i
					cut_nposts		=	cut_nposts	+	 len(df_cut.index)
					df_cut 			=	df_cut.drop(columns=['pre_Message','pre_Image_Text','pre_Description'])
					dfs.append(df_cut)	
					dfs_total.append(df_cut)	
					df_cut.to_csv(fake_path+"/"+fake+"_"+str(i)+"_.csv",quoting=csv.QUOTE_NONNUMERIC,index=False)	
					i 				=	i	+	1
					posts_id.append( list(df_cut['URL'])) 
			if(cib):
				cib_total		=	cib_total		+	(i-1)
				cib_total_posts	=	cib_total_posts	+	cut_nposts	
				f.write(fake+","+str(n_posts)+","+str(cib_total)+","+str(cib_total_posts)+"\n")	
				
				df_cut			=	pd.concat(dfs, join='outer', axis=0)	
				df_cut.to_csv(fake_path+"/"+fake+".csv",quoting=csv.QUOTE_NONNUMERIC,index=False)			
				print("Cib Total: " 		+str(cib_total))
				print("cib_total_posts: " 	+str(cib_total_posts))

				fake_posts.append({"fake":fake,"posts_ids":posts_id})

			df		=	pd.concat(dfs_total, join='outer', axis=0)	
			df.to_csv("./fakes/ALL_CIB.csv",quoting=csv.QUOTE_NONNUMERIC,index=False)		
			#create_graphs(df, fake_posts, "./fakes")
		
		except Exception as e:
			print(e)
			continue


	f.close()
if __name__ == "__main__":
   main(sys.argv[1:])
