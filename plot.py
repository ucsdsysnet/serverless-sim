import matplotlib.pyplot as plt
import numpy as np


def plot(epoch, metrics, stats, filename):
    fig, axs = plt.subplots(7, sharex=True, sharey=False, figsize=(6, 15))
    x = np.arange(0.5, epoch)
    # request
    axs[0].title.set_text('request')
    axs[0].hist(metrics['request'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # start
    axs[1].title.set_text('start')
    axs[1].hist(metrics['start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # running
    axs[2].title.set_text('load')
    axs[2].bar(x, stats['load'], 1, facecolor='k', alpha=0.75)
    # inqueue
    axs[3].title.set_text('inqueue')
    axs[3].bar(x, stats['inqueue'], 1, facecolor='k', alpha=0.75)
    # cold start
    axs[4].title.set_text('coldstart')
    axs[4].hist(metrics['cold-start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)    
    # evict
    axs[5].title.set_text('evict')
    axs[5].hist(metrics['evict'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # delay
    axs[6].title.set_text('average-delay')
    axs[6].bar(x, stats['average-delay'], 1, facecolor='k', alpha=0.75)

    fig.tight_layout()
    fig.savefig(filename)
