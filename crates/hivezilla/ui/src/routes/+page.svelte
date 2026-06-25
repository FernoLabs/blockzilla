<script lang="ts">
  import { onMount } from 'svelte';

  type CardLot = {
    id: string;
    name: string;
    set: string;
    grade: string;
    listingPrice: number;
    duration: number;
    image: string;
  };

  type Bid = {
    bidder: string;
    amount: number;
    time: string;
  };

  const lots: CardLot[] = [
    {
      id: 'base-charizard',
      name: 'Charizard',
      set: 'Base Set 4/102',
      grade: 'Near Mint',
      listingPrice: 1,
      duration: 18,
      image: 'https://images.pokemontcg.io/base1/4_hires.png'
    },
    {
      id: 'jungle-pikachu',
      name: 'Pikachu',
      set: 'Jungle 60/64',
      grade: 'Light Play',
      listingPrice: 1,
      duration: 16,
      image: 'https://images.pokemontcg.io/jungle/60_hires.png'
    },
    {
      id: 'fossil-dragonite',
      name: 'Dragonite',
      set: 'Fossil 4/62',
      grade: 'Near Mint',
      listingPrice: 1,
      duration: 20,
      image: 'https://images.pokemontcg.io/fossil/4_hires.png'
    },
    {
      id: 'base-blastoise',
      name: 'Blastoise',
      set: 'Base Set 2/102',
      grade: 'Excellent',
      listingPrice: 1,
      duration: 18,
      image: 'https://images.pokemontcg.io/base1/2_hires.png'
    },
    {
      id: 'base-venusaur',
      name: 'Venusaur',
      set: 'Base Set 15/102',
      grade: 'Near Mint',
      listingPrice: 1,
      duration: 18,
      image: 'https://images.pokemontcg.io/base1/15_hires.png'
    }
  ];

  let queue = $state([...lots]);
  let currentLot = $state<CardLot>(queue[0]);
  let currentPrice = $state(currentLot.listingPrice);
  let balance = $state(8);
  let topUpAmount = $state(20);
  let secondsLeft = $state(currentLot.duration);
  let message = $state('Auction started at $1. Add fake balance and bid when ready.');
  let bidHistory = $state<Bid[]>([]);

  const futureLots = $derived(queue.slice(1));
  const nextBid = $derived(currentPrice + 1);
  const buyoutPrice = $derived(Math.ceil(Math.max(currentPrice, currentLot.listingPrice) * 1.2));
  const canBid = $derived(balance >= nextBid && secondsLeft > 0);
  const canBuyout = $derived(balance >= buyoutPrice && secondsLeft > 0);
  const timeLabel = $derived(formatTime(secondsLeft));

  onMount(() => {
    const timer = window.setInterval(() => {
      if (secondsLeft > 0) {
        secondsLeft -= 1;
      }
    }, 1000);

    return () => window.clearInterval(timer);
  });

  function formatMoney(amount: number) {
    return `$${amount.toLocaleString('en-US')}`;
  }

  function formatTime(seconds: number) {
    const minutes = Math.floor(seconds / 60);
    const rest = seconds % 60;
    return `${minutes}:${rest.toString().padStart(2, '0')}`;
  }

  function topUp() {
    const amount = Math.max(1, Math.floor(topUpAmount));
    balance += amount;
    message = `Fake wallet topped up by ${formatMoney(amount)}.`;
  }

  function placeBid() {
    if (secondsLeft <= 0) {
      message = 'This auction has ended. Advance to the next card.';
      return;
    }

    if (balance < nextBid) {
      message = `Top up first. Next bid needs ${formatMoney(nextBid)}.`;
      return;
    }

    balance -= nextBid;
    currentPrice = nextBid;
    secondsLeft += 1;
    bidHistory = [
      {
        bidder: 'You',
        amount: currentPrice,
        time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
      },
      ...bidHistory
    ].slice(0, 6);
    message = `Bid placed at ${formatMoney(currentPrice)}. One second added.`;
  }

  function buyoutAndSkip() {
    if (secondsLeft <= 0) {
      message = 'This auction has already ended.';
      return;
    }

    if (balance < buyoutPrice) {
      message = `Skip needs ${formatMoney(buyoutPrice)} in your fake wallet.`;
      return;
    }

    balance -= buyoutPrice;
    message = `${currentLot.name} bought instantly for ${formatMoney(buyoutPrice)}.`;
    advanceLot();
  }

  function advanceLot() {
    const [, ...remaining] = queue;
    queue = remaining.length > 0 ? remaining : [...lots];
    currentLot = queue[0];
    currentPrice = currentLot.listingPrice;
    secondsLeft = currentLot.duration;
    bidHistory = [];
  }
</script>

<svelte:head>
  <title>Card Dollar Auction</title>
</svelte:head>

<main class="page">
  <header class="topbar">
    <div>
      <h1>Card Dollar Auction</h1>
      <p>Pokemon card auctions start at $1. Every valid bid raises the price by $1 and adds one second.</p>
    </div>

    <div class="wallet" aria-label="Fake wallet balance">
      <span>Balance</span>
      <strong>{formatMoney(balance)}</strong>
    </div>
  </header>

  <section class="auction-layout">
    <article class="card-preview" aria-label={`Current auction for ${currentLot.name}`}>
      <img src={currentLot.image} alt={`${currentLot.name} Pokemon card`} />
    </article>

    <section class="auction-panel">
      <div class="lot-heading">
        <div>
          <h2>{currentLot.name}</h2>
          <p>{currentLot.set} · {currentLot.grade}</p>
        </div>
        <div class={['timer', secondsLeft <= 5 && 'urgent']}>
          <span>Time left</span>
          <strong>{timeLabel}</strong>
        </div>
      </div>

      <div class="price-strip">
        <div>
          <span>Current price</span>
          <strong>{formatMoney(currentPrice)}</strong>
        </div>
        <div>
          <span>Next bid</span>
          <strong>{formatMoney(nextBid)}</strong>
        </div>
        <div>
          <span>Skip price</span>
          <strong>{formatMoney(buyoutPrice)}</strong>
        </div>
      </div>

      <div class="actions">
        <button class="primary" type="button" disabled={!canBid} onclick={placeBid}>
          Bid +$1
        </button>
        <button type="button" disabled={!canBuyout} onclick={buyoutAndSkip}>
          Skip auction
        </button>
        <button type="button" onclick={advanceLot}>
          Next card
        </button>
      </div>

      <form class="topup" onsubmit={(event) => { event.preventDefault(); topUp(); }}>
        <label for="top-up">Fake top up</label>
        <input id="top-up" type="number" min="1" step="1" bind:value={topUpAmount} />
        <button type="submit">Add funds</button>
      </form>

      <p class="message" aria-live="polite">{message}</p>

      <div class="history">
        <div class="section-title">
          <h3>Recent bids</h3>
          <span>{bidHistory.length}</span>
        </div>
        {#each bidHistory as bid (bid.time + bid.amount)}
          <div class="bid-row">
            <span>{bid.bidder}</span>
            <strong>{formatMoney(bid.amount)}</strong>
            <time>{bid.time}</time>
          </div>
        {:else}
          <p>No bids yet.</p>
        {/each}
      </div>
    </section>
  </section>

  <section class="future">
    <div class="section-title">
      <h2>Future cards</h2>
      <span>{futureLots.length}</span>
    </div>

    <div class="future-grid">
      {#each futureLots as lot (lot.id)}
        <article class="future-card">
          <img src={lot.image} alt={`${lot.name} Pokemon card`} />
          <div>
            <h3>{lot.name}</h3>
            <p>{lot.set}</p>
            <span>Starts at {formatMoney(lot.listingPrice)}</span>
          </div>
        </article>
      {/each}
    </div>
  </section>
</main>

<style>
  .page {
    width: min(1180px, calc(100vw - 32px));
    margin: 0 auto;
    padding: 28px 0 40px;
  }

  .topbar {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 24px;
    border-bottom: 1px solid #303035;
    padding-bottom: 22px;
  }

  h1,
  h2,
  h3,
  p {
    margin: 0;
  }

  h1 {
    font-size: 24px;
    font-weight: 720;
  }

  .topbar p {
    max-width: 660px;
    margin-top: 6px;
    color: #a7a7ad;
  }

  .wallet,
  .timer,
  .price-strip > div,
  .history,
  .future-card {
    border: 1px solid #303035;
    border-radius: 8px;
    background: #171719;
  }

  .wallet {
    min-width: 160px;
    padding: 12px 14px;
    text-align: right;
  }

  .wallet span,
  .timer span,
  .price-strip span,
  .future-card span {
    display: block;
    color: #a7a7ad;
    font-size: 13px;
  }

  .wallet strong {
    display: block;
    margin-top: 4px;
    font-size: 24px;
  }

  .auction-layout {
    display: grid;
    grid-template-columns: minmax(260px, 390px) minmax(0, 1fr);
    gap: 24px;
    margin-top: 24px;
    align-items: start;
  }

  .card-preview {
    border: 1px solid #303035;
    border-radius: 8px;
    background: #111113;
    padding: 18px;
  }

  .card-preview img {
    display: block;
    width: 100%;
    aspect-ratio: 734 / 1024;
    object-fit: contain;
  }

  .auction-panel {
    min-width: 0;
  }

  .lot-heading {
    display: flex;
    justify-content: space-between;
    gap: 18px;
  }

  .lot-heading h2 {
    font-size: 30px;
    line-height: 1.1;
  }

  .lot-heading p {
    margin-top: 7px;
    color: #a7a7ad;
  }

  .timer {
    min-width: 124px;
    padding: 11px 12px;
    text-align: right;
  }

  .timer strong {
    display: block;
    margin-top: 4px;
    font-size: 24px;
    font-variant-numeric: tabular-nums;
  }

  .timer.urgent {
    border-color: #b54747;
    color: #ffb3b3;
  }

  .price-strip {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 10px;
    margin-top: 22px;
  }

  .price-strip > div {
    min-height: 86px;
    padding: 13px;
  }

  .price-strip strong {
    display: block;
    margin-top: 8px;
    font-size: 28px;
    line-height: 1;
  }

  .actions {
    display: grid;
    grid-template-columns: 1.3fr 1fr 1fr;
    gap: 10px;
    margin-top: 14px;
  }

  .primary {
    border-color: #00c792;
    background: #00d4aa;
    color: #0b1411;
  }

  .primary:hover:not(:disabled) {
    background: #21e0b9;
  }

  .topup {
    display: grid;
    grid-template-columns: auto minmax(120px, 160px) auto;
    align-items: end;
    gap: 10px;
    margin-top: 14px;
  }

  .topup label {
    color: #cfcfd2;
    font-weight: 600;
  }

  .message {
    margin-top: 14px;
    color: #d8d8da;
    min-height: 22px;
  }

  .history {
    margin-top: 18px;
    overflow: hidden;
  }

  .section-title {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    min-height: 48px;
    border-bottom: 1px solid #303035;
    padding: 0 14px;
  }

  .section-title h2,
  .section-title h3 {
    font-size: 15px;
    font-weight: 700;
  }

  .section-title span {
    color: #a7a7ad;
  }

  .bid-row {
    display: grid;
    grid-template-columns: 1fr auto auto;
    gap: 14px;
    align-items: center;
    min-height: 42px;
    border-bottom: 1px solid #29292d;
    padding: 0 14px;
  }

  .bid-row:last-child {
    border-bottom: 0;
  }

  .bid-row time,
  .history p {
    color: #a7a7ad;
  }

  .history p {
    padding: 14px;
  }

  .future {
    margin-top: 24px;
    border: 1px solid #303035;
    border-radius: 8px;
    background: #111113;
    overflow: hidden;
  }

  .future-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    padding: 14px;
  }

  .future-card {
    display: grid;
    grid-template-columns: 72px minmax(0, 1fr);
    gap: 12px;
    padding: 10px;
    min-width: 0;
  }

  .future-card img {
    width: 72px;
    aspect-ratio: 734 / 1024;
    object-fit: contain;
  }

  .future-card h3 {
    font-size: 15px;
  }

  .future-card p {
    margin: 4px 0 8px;
    color: #a7a7ad;
    font-size: 13px;
  }

  @media (max-width: 920px) {
    .topbar,
    .lot-heading {
      flex-direction: column;
    }

    .wallet,
    .timer {
      width: 100%;
      text-align: left;
    }

    .auction-layout {
      grid-template-columns: 1fr;
    }

    .card-preview {
      max-width: 360px;
    }

    .price-strip,
    .actions,
    .future-grid {
      grid-template-columns: 1fr;
    }

    .topup {
      grid-template-columns: 1fr;
      align-items: stretch;
    }
  }
</style>
