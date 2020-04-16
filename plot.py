from statistics import mean, stdev
import matplotlib.pyplot as plt
import numpy as np


def plot(epoch, metrics, stats, filename):
    fig, axs = plt.subplots(5, 2, sharex=True, sharey=False, figsize=(12, 10))
    x = np.arange(0.5, epoch)
    current = 0
    # request
    axs[0][0].title.set_text('request')
    axs[0][0].hist(metrics['request'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)

    # fig.tight_layout()
    # fig.savefig(filename)

    # start
    current += 1
    axs[0][1].title.set_text('start')
    axs[0][1].hist(metrics['start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # finish
    current += 1
    axs[1][0].title.set_text('finish')
    axs[1][0].hist(metrics['finish'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # running
    current += 1
    axs[1][1].bar(x, stats['load'], 1, facecolor='k', alpha=0.75)
    axs[1][1].title.set_text('load: '+('%.3f' % mean(stats['load']))+'±'+('%.3f' % stdev(stats['load'])))
    # inqueue
    current += 1
    axs[2][0].title.set_text('inqueue')
    axs[2][0].bar(x, stats['inqueue'], 1, facecolor='k', alpha=0.75)
    # cold start
    current += 1
    axs[2][1].title.set_text('cold-start')
    axs[2][1].hist(metrics['cold-start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)    
    # evict
    current += 1
    axs[3][0].title.set_text('evict')
    axs[3][0].hist(metrics['evict'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # delay
    current += 1
    axs[3][1].bar(x, stats['delay'], 1, facecolor='k', alpha=0.75)
    axs[3][1].title.set_text('average delay: '+('%.3f'%mean(stats['delay']))+'±'+('%.3f'%stdev(stats['delay'])))

    # non-home
    current += 1
    axs[4][0].title.set_text('non-home')
    axs[4][0].hist(metrics['non-home'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # distance
    current += 1
    axs[4][1].bar(x, stats['distance'], 1, facecolor='k', alpha=0.75)
    axs[4][1].title.set_text('average distance: '+('%.2f'%mean(stats['distance']))+'±'+('%.2f'%stdev(stats['distance'])))

    fig.tight_layout()
    plt.show()
    # fig.savefig(filename)
