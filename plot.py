from statistics import mean, stdev
import matplotlib.pyplot as plt
import numpy as np


def plot(epoch, metrics, stats, filename):
    nrow = 5
    ncol = 2
    fig, axs = plt.subplots(nrow, ncol, sharex=True, sharey=False, figsize=(12, 10))
    def axi(i):
        return axs[i//ncol][i%ncol]
    x = np.arange(0.5, epoch)

    current = 0
    # request
    axi(current).hist(metrics['request'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    axi(current).title.set_text('request: total '+str(len(metrics['request'])))
    # start
    current += 1
    axi(current).title.set_text('start')
    axi(current).hist(metrics['start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # finish
    current += 1
    axi(current).title.set_text('finish')
    axi(current).hist(metrics['finish'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # load
    current += 1
    def fmt_stat(y):
        return ('%.3f' % mean(y))+'±'+('%.3f' % stdev(y))
    # axi(current).bar(x, stats['load'], 1, facecolor='k', alpha=0.75)    
    userload = [(load - sys) for load, sys in zip(stats['load'], stats['sys-load'])]
    axi(current).bar(x, stats['sys-load'], 1, facecolor='r', alpha=0.75) # cold start (system) load
    axi(current).bar(x, userload, 1, bottom=stats['sys-load'], facecolor='g', alpha=0.75) # user load
    axi(current).bar(x, stats['sb-load'], 1, bottom=stats['load'], facecolor='y', alpha=0.75) # sandbox load
    axi(current).title.set_text('load: sys ' + fmt_stat(stats['sys-load']) + ', user ' + fmt_stat(userload) + 
        ', sandbox ' + fmt_stat(stats['sb-load']))
    # inqueues
    current += 1
    axi(current).title.set_text('inqueue')
    axi(current).bar(x, stats['inqueue'], 1, facecolor='k', alpha=0.75)
    # cold start
    current += 1
    axi(current).hist(metrics['cold-start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)    
    axi(current).title.set_text('cold-start: total '+str(len(metrics['cold-start'])))
    # evict
    current += 1
    axi(current).hist(metrics['evict'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    axi(current).title.set_text('evict: total '+str(len(metrics['evict'])))
    # delay
    current += 1
    axi(current).bar(x, stats['delay'], 1, facecolor='k', alpha=0.75)
    axi(current).title.set_text('average delay: '+('%.3f'%mean(stats['delay']))+'±'+('%.3f'%stdev(stats['delay'])))
    # non-home
    current += 1
    axi(current).title.set_text('non-home')
    axi(current).hist(metrics['non-home'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # distance
    current += 1
    axi(current).bar(x, stats['distance'], 1, facecolor='k', alpha=0.75)
    axi(current).title.set_text('average distance: '+('%.2f'%mean(stats['distance']))+'±'+('%.2f'%stdev(stats['distance'])))

    fig.tight_layout()
    # plt.show()
    fig.savefig(filename)
