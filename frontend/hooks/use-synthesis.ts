import { useQuery } from '@tanstack/react-query';
import { v3Api } from '@/lib/api';

export interface SynthesisData {
    cluster_distribution: {
        Emerging: number;
        Mature: number;
        "High Churn": number;
    };
    total_districts: number;
    ghost_districts: any[];
    high_utilization_pincodes: any[];
}

export const useSynthesis = () => {
    return useQuery({
        queryKey: ['synthesis'],
        queryFn: async () => {
            const { data } = await v3Api.get<{ success: boolean; data: SynthesisData }>('/intel/synthesis');
            return data.data;
        },
        // Refresh every 30 seconds
        refetchInterval: 30000,
    });
};
