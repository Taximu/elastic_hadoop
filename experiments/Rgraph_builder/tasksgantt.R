library('plyr')
library(ggplot2)
library('ibdreg')
library("lattice")

###FONT SETTINGS
trellis.par.get("fontsize")

df <- NULL
for (expNum in c(1, 2, 3, 4, 5)) { 
    for (bench in c('wordcount', 'sort', 'pagerank')) {
        filename <- paste('expm', expNum, '/', bench, '.stat.perNM', sep = "")
        dftemp <- read.table(filename, header = T, fill = T)
        dftemp <- na.omit(dftemp)
        dftemp$bench <- bench
        dftemp$expNum <- paste("expm", expNum, sep = "")
        dftemp$conf <- paste("expm", expNum, bench, sep = "_")
        df <- rbind(df, dftemp)
    }
}
dftemp <- NULL

##remove preparejob
df$jobid <- as.character(df$jobid)
df$jobid <- gsub("^.+_", "", df$jobid)
df$jobid <- as.numeric(df$jobid)


###fix time
startTimes <- aggregate(list(start = df$attemptstartTime), df[, c('bench', 'conf')], min)

for (m in unique(df$conf)) {
    for (b in unique(df$bench)) {
        startTime <- startTimes$start[startTimes$conf == m & startTimes$bench == b ]
	    df$attemptstartTime[df$conf == m & df$bench == b ] <- df$attemptstartTime[df$conf == m & df$bench == b ] - startTime
	    df$attemptfinishTime[df$conf == m & df$bench == b ] <- df$attemptfinishTime[df$conf == m & df$bench == b ] - startTime
	    df$shuffleFinishTime[df$conf == m & df$bench == b ] <- df$shuffleFinishTime[df$conf == m & df$bench == b ] - startTime
	}
}
df$attemptstartTime <- df$attemptstartTime / 1000 ##get seconds
df$attemptfinishTime <- df$attemptfinishTime / 1000 ##get seconds
df$shuffleFinishTime <- df$shuffleFinishTime / 1000 ##get seconds

dftemp <- subset(df, tasktype == 'REDUCE')
dftemp$attemptstartTime <- dftemp$shuffleFinishTime

dftemp1 <- subset(df, tasktype == 'REDUCE')
dftemp1$tasktype <- 'SHUFFLE'
dftemp1$attemptfinishTime <- dftemp1$shuffleFinishTime
dftemp <- rbind(dftemp, dftemp1)
df<-subset(df, tasktype=='MAP')
df <- rbind(df, dftemp)
dftemp <- NULL
dftemp1 <- NULL



df$taskid <- ifelse( grepl('_m_', df$taskid),  paste('M', gsub("^.+_", "", df$taskid), sep = "_"), paste('R', gsub("^.+_", "", df$taskid), sep = "_"))
df$taskid <- gsub("M_", "", df$taskid)
df$taskid <- gsub("R_", "", df$taskid)
df$taskid <- as.numeric(df$taskid)

###since task id starts from 0 
df$taskid <- df$taskid + 1

df$taskid[df$tasktype %in% c('REDUCE', 'SHUFFLE' )] <- df$taskid[df$tasktype %in% c('REDUCE', 'SHUFFLE')] + max(df$taskid[df$tasktype == 'MAP'])

df$vm <- gsub(".inf.tu-dresden.de:8042", "", df$nodeHttpAddress)

p <- ggplot(df,  aes(colour = vm))
p <- p + theme_bw()
p <- p + geom_segment(aes(x = attemptstartTime, xend = attemptfinishTime,  y = taskid, yend = taskid, linetype = tasktype),  size = 0.5)
P <- p + scale_linetype_manual(values = c("solid", "1F", "F1" ))
p <- p + geom_point(aes(x = attemptstartTime, y = taskid), size = 2, shape = '|')
p <- p + geom_point(aes(x = attemptfinishTime, y = taskid), size = 2, shape = '|')
p <- p + theme(legend.position = "top", panel.grid.major = element_blank())
p <- p + xlab("Task duration (sec)")
p <- p + ylab("Tasks")
p <- p + facet_grid(expNum~bench, scales = 'free_x')
ggsave(file = "tasksgantt.svg", plot = p)
