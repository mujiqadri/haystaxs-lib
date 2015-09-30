package com.haystack;

import com.haystack.service.MainService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.server.ExportException;

/**
 * Created by qadrim on 15-04-23.
 */
public class Main {

    static Logger log = LoggerFactory.getLogger(Main.class.getName());
    static MainService mainService = new MainService();

    public static void main(String[] args) {
            mainService.run();
    }
}
