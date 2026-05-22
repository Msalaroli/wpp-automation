package com.apijavaspring.wpp_automation.core;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class PhoneNumberVariants {

    private PhoneNumberVariants() {
    }

    public static String digitsOnly(String raw) {
        return raw == null ? "" : raw.replaceAll("\\D", "");
    }

    public static List<String> brazilianVariants(String raw) {
        LinkedHashSet<String> variants = new LinkedHashSet<>();

        String digits = digitsOnly(raw);
        if (digits.isBlank()) return List.of();

        variants.add(digits);

        String local = digits;
        if (digits.startsWith("55") && digits.length() > 2) {
            local = digits.substring(2);
            variants.add(local);
        } else {
            variants.add("55" + local);
        }

        addLocalNineVariants(variants, local);

        return new ArrayList<>(variants);
    }

    private static void addLocalNineVariants(LinkedHashSet<String> variants, String local) {
        if (local.matches("^\\d{2}9\\d{8}$")) {
            String withoutNine = local.substring(0, 2) + local.substring(3);
            variants.add(withoutNine);
            variants.add("55" + withoutNine);
            return;
        }

        if (local.matches("^\\d{2}\\d{8}$")) {
            String withNine = local.substring(0, 2) + "9" + local.substring(2);
            variants.add(withNine);
            variants.add("55" + withNine);
        }
    }
}
