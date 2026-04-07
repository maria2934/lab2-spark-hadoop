# Установка Java
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_482.jdk/Contents/Home

# Логи (создадим папку)
export HADOOP_LOG_DIR=/Users/mariia/Desktop/lab2-spark-hadoop/hadoop/logs
mkdir -p $HADOOP_LOG_DIR

# PID-файлы
export HADOOP_PID_DIR=/tmp

# Пользователи
export HDFS_NAMENODE_USER=mariia
export HDFS_DATANODE_USER=mariia
export HDFS_SECONDARYNAMENODE_USER=mariia

# Чтобы SSH передавал переменные
export HADOOP_SSH_OPTS="-o SendEnv JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR"
