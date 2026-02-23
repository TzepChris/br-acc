import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { type StatsResponse, getStats } from "@/api/client";
import { useCountUp } from "@/hooks/useCountUp";

import styles from "./StatsBar.module.css";

interface StatItemProps {
  target: number;
  label: string;
  suffix?: string;
}

function StatItem({ target, label, suffix = "" }: StatItemProps) {
  const { ref, value } = useCountUp(target);

  return (
    <div className={styles.item} ref={ref}>
      <span className={styles.number}>
        {value.toLocaleString()}
        {suffix}
      </span>
      <span className={styles.label}>{label}</span>
    </div>
  );
}

function formatLargeNumber(n: number): { value: number; suffix: string } {
  if (n >= 1_000_000) return { value: Math.round(n / 100_000) / 10, suffix: "M" };
  if (n >= 1_000) return { value: Math.round(n / 100) / 10, suffix: "K" };
  return { value: n, suffix: "" };
}

export function StatsBar() {
  const { t } = useTranslation();
  const [stats, setStats] = useState<StatsResponse | null>(null);

  useEffect(() => {
    getStats().then(setStats).catch(() => {});
  }, []);

  if (!stats) {
    return (
      <div className={styles.bar}>
        <div className={styles.inner}>
          <div className={styles.item}>
            <span className={styles.number}>—</span>
            <span className={styles.label}>{t("common.loading")}</span>
          </div>
        </div>
      </div>
    );
  }

  const nodes = formatLargeNumber(stats.total_nodes);
  const rels = formatLargeNumber(stats.total_relationships);

  return (
    <div className={styles.bar}>
      <div className={styles.inner}>
        <StatItem target={nodes.value} suffix={nodes.suffix} label={t("landing.stats.entities")} />
        <StatItem target={rels.value} suffix={rels.suffix} label={t("landing.stats.connections")} />
        <StatItem target={stats.data_sources} label={t("landing.stats.dataSources")} />
      </div>
    </div>
  );
}
