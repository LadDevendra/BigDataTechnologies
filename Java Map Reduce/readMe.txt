
Commands to run the program..

Q1)
 File: PaloAlto.Java
 class: PaloAlto.class
 jar: Assignment1
 package: BigData

Instructions for running:

1) Delete existing output directories if any
2) Upload the jar using putty/cyber duck
3) hadoop jar Assignment1.jar BigData.PaloAlto /dvl160030/dataset/business.csv /dvl160030/output
4) output: hadoop dfs -cat /dvl160030/output/*

Q2)
 File: TopTenBusiness.Java
 class: TopTenBusiness.class
 jar: Assignment1
 package: BigData

Instructions for running:

1) Delete existing output directories if any
2) Upload the jar using putty/cyber duck
3)  hadoop jar Assignment1.jar BigData.TopTenBusiness /dvl160030/dataset/review.csv /dvl160030/output1 /dvl160030/output2
4) output: hadoop dfs -cat /dvl160030/output2/*

Q3)
 File: TopTenBusiness_Q3.Java
 class: TopTenBusiness_Q3.class
 jar: Assignment1
 package: BigData

Instructions for running:

1) Delete existing output directories if any
2) Upload the jar using putty/cyber duck
3) hadoop jar Assignment1.jar BigData.TopTenBusiness_Q3 /dvl160030/dataset/review.csv /dvl160030/output1 /dvl160030/output2 /dvl160030/dataset/business.csv /dvl160030/output3
4) output: hadoop dfs -cat /dvl160030/output3/*


Q4)
 File: UserId.Java
 class: UserId.class
 jar: Assignment1
 package: BigData

Instructions for running:

1) Delete existing output directories if any
2) Upload the jar
3) hadoop jar Assignment1.jar BigData.UserId /dvl160030/dataset/business.csv /dvl160030/dataset/review.csv /dvl160030/output
4) output: hadoop dfs -cat /dvl160030/output/*

