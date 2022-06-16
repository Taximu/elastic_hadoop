library('plyr')
library(ggplot2)

df <- NULL
for (expNum in c(1, 2, 3, 4)) { 
    for (bench in c('sort', 'wordcount', 'pagerank')) {
        filename <- paste("expm", expNum, '/', bench, '.stats', sep = "")
        dftemp <- read.table(filename, header = T, fill = T)
        dftemp <- na.omit(dftemp)
        dftemp$bench <- bench
        dftemp$expNum <- paste("expm", expNum, sep = "")
        dftemp$conf <- paste("expm", expNum, bench, sep = "_")
        df <- rbind(df, dftemp)
    }
}
dftemp <- NULL

###fix time
for (m in unique(df$conf)) {
    for (v in unique(df$vm)) {
        df$time[df$conf == m & df$vm == v ] <- df$timestamp[df$conf == m & df$vm == v] - df$timestamp[df$conf == m & df$vm == v ][1]
    }
}

##JOB COMPLETION TIME##
execTime <- aggregate(list(time = df$time), df[, c('bench', 'expNum')], max)

ggplot(data = execTime, aes(x = expNum, y = time, fill = expNum))+ 
     geom_bar(stat = 'identity')+
     geom_text(aes(label = round(time, digits = 2)), position = position_dodge(width = 0.9), vjust = -0.25)+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("job completion time (sec)")+
     facet_grid(.~bench, scales = 'free_y')
     ggsave(file = "jct.png")


##NETWORK I/O##
df$recv <- df$eth0_rxkB.s / 1024#mb
df$send <- df$eth0_txkB.s / 1024#mb


netavg <- subset(df, vm %in% c('stream2', 'stream4'))
netavg <- ddply(netavg, c('bench', 'expNum'), summarise,
               N    = length(recv),
               mean = mean(recv),
               sd   = sd(recv),
               se   = sd / sqrt(N))


ggplot(data = netavg, aes(x = expNum, y = mean, fill = expNum))+ 
     geom_bar(stat = 'identity')+
     geom_errorbar(aes(ymin = mean-se, ymax = mean + se), position = 'dodge', width = 0.25)+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("static part - network receive (MB/sec)")+
     facet_grid(bench~., scales = 'free_y')
     ggsave(file = "net_receive_exp1-4.png")


netavg <- subset(df, vm %in% c('stream2', 'stream4'))
netavg <- ddply(netavg, c('bench', 'expNum'), summarise,
               N    = length(send),
               mean = mean(send),
               sd   = sd(send),
               se   = sd / sqrt(N))


ggplot(data = netavg, aes(x = expNum, y = mean, fill = expNum))+ 
     geom_bar(stat = 'identity')+
     geom_errorbar(aes(ymin = mean - se, ymax = mean + se), position = 'dodge', width = 0.25)+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("static part - network send (MB/sec)")+
     facet_grid(bench~., scales = 'free_y')
     ggsave(file = "net_send_exp1-4.png")


net <- subset(df, expNum %in% c('expm1', 'expm4') & vm %in% c('stream3', 'stream2', 'stream4'))
net$vmRoles <- factor(net$vm, labels = c('master', 'coreNode1', 'coreNode2'))
ggplot(data = net, aes(x = time, y = recv, colour = expNum)) + 
     geom_line()+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("network receive (MB/sec)")+
     facet_grid(vmRoles~bench, scales = 'free_y')
     ggsave(file = "net_rec_exp1&4.png")


##SWAPUSED##
df$swap <- df$kbswpused / 1024#mb

swp <- subset(df, vm %in% c('stream2', 'stream4'))
swp <- ddply(swp, c('bench', 'expNum'), summarise,
               N    = length(swap),
               mean = mean(swap),
               sd   = sd(swap),
               se   = sd / sqrt(N))


ggplot(data = swp, aes(x = expNum, y = mean, fill = expNum))+ 
     geom_bar(stat='identity')+
     geom_errorbar(aes(ymin = mean - se, ymax = mean + se), position = 'dodge', width = 0.25)+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("static part - swapmem (MB/sec)")+
     facet_grid(bench~., scales = 'free_y')
     ggsave(file = "kbswpused.png")


##CPU##
df$iowt <- ifelse(df$iowait < 100, df$iowait, 0)
df$cpu <- 100 - df$idle - df$iowt
iow <- subset(df, vm %in% c('stream2', 'stream4'))
iow <- ddply(iow, c('bench', 'expNum'), summarise,
               N    = length(cpu),
               mean = mean(cpu),
               sd   = sd(cpu),
               se   = sd / sqrt(N))


ggplot(data = iow, aes(x = expNum, y = mean, fill = expNum))+ 
     geom_bar(stat = 'identity')+
     geom_errorbar(aes(ymin = mean - se, ymax = mean + se), position = 'dodge', width = 0.25)+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("static part - cpu (%)")+
     facet_grid(bench~., scales = 'free_y')
     ggsave(file = "cpu.png")


##DISK I/O##
df$write <- df$sda_wr_sec.s / 1024#mb
df$read <- df$sda_rd_sec.s / 1024#mb


netavg <- subset(df, vm %in% c('stream2', 'stream4'))
netavg <- ddply(netavg, c('bench', 'expNum'), summarise,
               N    = length(write),
               mean = mean(write),
               sd   = sd(write),
               se   = sd / sqrt(N))


ggplot(data = netavg, aes(x = expNum, y = mean, fill = expNum))+ 
     geom_bar(stat = 'identity')+
     geom_errorbar(aes(ymin = mean - se, ymax = mean + se), position = 'dodge', width = 0.25)+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("static part - network receive (MB/sec)")+
     facet_grid(bench~., scales = 'free_y')
     ggsave(file = "disk_write_exp1-4.png")


netavg <- subset(df, vm %in% c('stream2', 'stream4'))
netavg <- ddply(netavg, c('bench', 'expNum'), summarise,
               N    = length(read),
               mean = mean(read),
               sd   = sd(read),
               se   = sd / sqrt(N))


ggplot(data = netavg, aes(x = expNum, y = mean, fill = expNum))+ 
     geom_bar(stat = 'identity')+
     geom_errorbar(aes(ymin = mean - se, ymax = mean + se), position = 'dodge', width = 0.25)+
     theme(axis.text.x = element_blank(), legend.position = "top")+
     ylab("static part - network send (MB/sec)")+
     facet_grid(bench~., scales = 'free_y')
     ggsave(file = "disk_read_exp1-4.png")