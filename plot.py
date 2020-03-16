import matplotlib.pyplot as plt
import numpy as np


def plot(epoch, metrics, stats, filename):
    fig, axs = plt.subplots(10, sharex=True, sharey=False, figsize=(6, 20))
    x = np.arange(0.5, epoch)
    current = 0
    # request
    axs[current].title.set_text('request')
    axs[current].hist(metrics['request'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # start
    current += 1
    axs[current].title.set_text('start')
    axs[current].hist(metrics['start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # finish
    current += 1
    axs[current].title.set_text('finish')
    axs[current].hist(metrics['finish'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # running
    current += 1
    axs[current].title.set_text('load')
    axs[current].bar(x, stats['load'], 1, facecolor='k', alpha=0.75)
    # inqueue
    current += 1
    axs[current].title.set_text('inqueue')
    axs[current].bar(x, stats['inqueue'], 1, facecolor='k', alpha=0.75)
    # cold start
    current += 1
    axs[current].title.set_text('cold-start')
    axs[current].hist(metrics['cold-start'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)    
    # evict
    current += 1
    axs[current].title.set_text('evict')
    axs[current].hist(metrics['evict'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # delay
    current += 1
    axs[current].title.set_text('average delay')
    axs[current].bar(x, stats['delay'], 1, facecolor='k', alpha=0.75)
    # non-home
    current += 1
    axs[current].title.set_text('non-home')
    axs[current].hist(metrics['non-home'], bins=epoch+1, range=(0, epoch+1), density=False, facecolor='k', alpha=0.75)
    # distance
    current += 1
    axs[current].title.set_text('average distance')
    axs[current].bar(x, stats['distance'], 1, facecolor='k', alpha=0.75)

    fig.tight_layout()
    fig.savefig(filename)
